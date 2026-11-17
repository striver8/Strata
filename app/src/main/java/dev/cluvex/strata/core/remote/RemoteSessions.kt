package dev.cluvex.strata.core.remote

import dev.cluvex.strata.R
import dev.cluvex.strata.core.i18n.tr
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap

class RemoteSessions(
    private val lookup: suspend (String) -> RemoteConnection?,
    private val onTrusted: suspend (RemoteConnection) -> Unit = {},
    private val factory: (RemoteConnection) -> RemoteFileSystem = RemoteFileSystems::create,
) {
    private class Session(var connection: RemoteConnection) {
        var fs: RemoteFileSystem? = null
        val lock = Mutex()
    }

    private val sessions = ConcurrentHashMap<String, Session>()

    /** Dedicated transfer connections so parallel uploads/downloads don't queue behind the browsing session. */
    private class WorkerPool {
        val permits = Semaphore(WORKER_LIMIT)
        val guard = Mutex()
        val idle = ArrayDeque<RemoteFileSystem>()
    }

    private val pools = ConcurrentHashMap<String, WorkerPool>()

    suspend fun name(connectionId: String): String? = session(connectionId)?.connection?.let { it.name.ifBlank { it.host } }

    suspend fun <T> use(connectionId: String, block: suspend (RemoteFileSystem) -> T): T {
        val session = session(connectionId) ?: throw IOException(tr(R.string.remote_storage_gone))
        return session.lock.withLock {
            val fs = session.fs?.takeIf { it.isConnected } ?: connect(session)
            try {
                block(fs)
            } catch (e: IOException) {
                if (!dropped(fs, e)) throw e
                runCatching { fs.close() }
                session.fs = null
                block(connect(session))
            }
        }
    }

    /**
     * Runs [block] on a dedicated worker connection (up to [WORKER_LIMIT] in parallel per storage).
     * Workers never hold the browsing session's lock, so listing and transferring proceed concurrently.
     */
    suspend fun <T> useWorker(connectionId: String, block: suspend (RemoteFileSystem) -> T): T {
        val session = session(connectionId) ?: throw IOException(tr(R.string.remote_storage_gone))
        val pool = pools.getOrPut(connectionId) { WorkerPool() }
        pool.permits.acquire()
        var fs: RemoteFileSystem? = null
        var reusable = false
        try {
            fs = pool.guard.withLock {
                while (true) {
                    val idle = pool.idle.removeFirstOrNull() ?: break
                    if (idle.isConnected) return@withLock idle
                    runCatching { idle.close() }
                }
                null
            } ?: open(session)
            try {
                val result = block(fs)
                reusable = true
                return result
            } catch (e: IOException) {
                if (!dropped(fs, e)) {
                    reusable = true
                    throw e
                }
                runCatching { fs.close() }
                fs = open(session)
                val result = block(fs)
                reusable = true
                return result
            }
        } finally {
            val current = fs
            if (current != null) {
                if (reusable && current.isConnected && pools[connectionId] === pool) {
                    pool.guard.withLock { pool.idle.addLast(current) }
                } else {
                    runCatching { current.close() }
                }
            }
            pool.permits.release()
        }
    }

    /** Closes idle worker connections after a transfer batch; in-flight workers close themselves on return. */
    fun releaseWorkers(connectionId: String) {
        val pool = pools.remove(connectionId) ?: return
        pool.idle.forEach { runCatching { it.close() } }
        pool.idle.clear()
    }

    suspend fun unlock(connectionId: String, password: String) = reconfigure(connectionId) { it.copy(password = password) }

    suspend fun trust(connectionId: String, presented: String) = reconfigure(connectionId) { it.copy(hostKey = presented) }

    private suspend fun reconfigure(connectionId: String, change: (RemoteConnection) -> RemoteConnection) {
        val session = session(connectionId) ?: return
        session.lock.withLock {
            session.fs?.let { runCatching { it.close() } }
            session.fs = null
            session.connection = change(session.connection)
        }
    }

    fun forget(connectionId: String) {
        releaseWorkers(connectionId)
        sessions.remove(connectionId)?.fs?.let { runCatching { it.close() } }
    }

    fun closeAll() {
        pools.keys.toList().forEach { releaseWorkers(it) }
        sessions.values.forEach { s -> s.fs?.let { runCatching { it.close() } } }
        sessions.clear()
    }

    private suspend fun session(id: String): Session? =
        sessions[id] ?: lookup(id)?.let { c -> sessions.getOrPut(id) { Session(c) } }

    private suspend fun connect(session: Session): RemoteFileSystem = open(session).also { session.fs = it }

    private suspend fun open(session: Session): RemoteFileSystem {
        val c = session.connection
        if (!c.anonymous && c.password.isEmpty()) throw RemotePasswordNeededException(c)
        val fs = factory(c)
        try {
            fs.connect()
        } catch (e: Exception) {
            runCatching { fs.close() }
            throw e
        }
        fs.newlyTrustedHostKey()?.let { key ->
            session.connection = c.copy(hostKey = key)
            onTrusted(session.connection)
        }
        return fs
    }

    private fun dropped(fs: RemoteFileSystem, e: IOException): Boolean =
        !fs.isConnected || e is SocketException || e.javaClass.simpleName.contains("ConnectionClosed")

    companion object {
        const val WORKER_LIMIT = 4
    }
}
