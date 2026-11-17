package dev.cluvex.strata.core.remote

import dev.cluvex.strata.R
import dev.cluvex.strata.core.fs.Conflict
import dev.cluvex.strata.core.fs.ConflictDecision
import dev.cluvex.strata.core.fs.ConflictInfo
import dev.cluvex.strata.core.fs.ConflictResolver
import dev.cluvex.strata.core.fs.FsUtil
import dev.cluvex.strata.core.fs.OpProgress
import dev.cluvex.strata.core.fs.identicalFile
import dev.cluvex.strata.core.i18n.tr
import dev.cluvex.strata.core.model.FileItem
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.IOException

sealed interface Place {
    data class Local(val dir: String) : Place
    data class Remote(val connectionId: String, val dir: String) : Place

    companion object {
        fun of(path: String): Place = RemoteUri.parse(path)?.let { (id, p) -> Remote(id, p) } ?: Local(path)
    }
}

object RemoteTransfers {

    fun transfer(
        items: List<FileItem>,
        to: Place,
        move: Boolean,
        conflict: Conflict,
        sessions: RemoteSessions,
        tempDir: File,
        resolver: ConflictResolver? = null,
        skipIdentical: Boolean = true,
    ): Flow<OpProgress> = channelFlow {
        val sources = items.map(Source::of)
        var totalBytes = 0L
        var totalFiles = 0
        for (s in sources) {
            val (b, f) = measure(s, sessions)
            totalBytes += b
            totalFiles += f
        }
        val progress = Progress(totalBytes, totalFiles) { trySend(it) }
        val results = Results()
        val conflicts = ConflictPolicy(conflict, resolver, skipIdentical)
        val permits = Semaphore(RemoteSessions.WORKER_LIMIT)
        try {
            // Top-level items run concurrently; directory leaves share the same permit pool.
            // A directory never holds a permit while its children wait for one, so no deadlock.
            coroutineScope {
                for (s in sources) {
                    launch {
                        ensureActive()
                        try {
                            val result = transferOne(s, to, move, conflicts, sessions, tempDir, progress, permits)
                            if (result == null) results.skip(s.name) else results.ok(result)
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            results.fail("${s.name}: ${e.message ?: e.javaClass.simpleName}")
                        }
                    }
                }
            }
            send(OpProgress.Finished(results.okCount, results.failed, results.skipped, results.created))
        } finally {
            (to as? Place.Remote)?.let { sessions.releaseWorkers(it.connectionId) }
            sources.filterIsInstance<Source.Remote>().map { it.id }.distinct().forEach { sessions.releaseWorkers(it) }
        }
    }.flowOn(Dispatchers.IO)

    fun delete(items: List<FileItem>, sessions: RemoteSessions): Flow<OpProgress> = channelFlow {
        var ok = 0
        val failed = ArrayList<String>()
        items.forEachIndexed { i, item ->
            ensureActive()
            val location = RemoteUri.parse(item.path)
            if (location == null) {
                failed += tr(R.string.remote_not_on_storage_item, item.name)
                return@forEachIndexed
            }
            trySend(OpProgress.Running(item.name, 0, 0, i, items.size))
            try {
                sessions.use(location.first) { fs -> fs.delete(location.second, recursive = item.isDirectory) }
                ok++
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                failed += "${item.name}: ${e.message ?: e.javaClass.simpleName}"
            }
        }
        send(OpProgress.Finished(ok, failed))
    }.flowOn(Dispatchers.IO)

    private suspend fun transferOne(
        s: Source,
        to: Place,
        move: Boolean,
        conflicts: ConflictPolicy,
        sessions: RemoteSessions,
        tempDir: File,
        progress: Progress,
        permits: Semaphore,
    ): String? = when (s) {
        is Source.Local -> when (to) {
            is Place.Local -> throw IllegalArgumentException("Local copies go through FileOperations")
            is Place.Remote -> {
                val target = sessions.use(to.connectionId) { fs ->
                    remoteTarget(fs, to.dir, s.name, s.isDirectory, s.file.length(), s.file.lastModified(), conflicts)
                } ?: return null
                uploadTree(to.connectionId, s.file, target, sessions, progress, permits)
                if (move) deleteLocal(s.file)
                RemoteUri.of(to.connectionId, target)
            }
        }
        is Source.Remote -> when (to) {
            is Place.Local -> {
                val target = localTarget(File(to.dir), s.name, s.isDirectory, s.size, s.modified, conflicts) ?: return null
                downloadTree(s.id, s.path, s.isDirectory, target, sessions, progress, permits)
                if (move) sessions.use(s.id) { fs -> fs.delete(s.path, recursive = s.isDirectory) }
                target.path
            }
            is Place.Remote -> when {
                move && s.id == to.connectionId && RemotePaths.parent(s.path) == RemotePaths.normalize(to.dir) -> null
                move && s.id == to.connectionId -> sessions.use(s.id) { fs ->
                    val target = remoteTarget(fs, to.dir, s.name, s.isDirectory, s.size, s.modified, conflicts) ?: return@use null
                    fs.rename(s.path, target)
                    progress.fileDone(s.name, s.size)
                    RemoteUri.of(to.connectionId, target)
                }
                else -> viaTemp(s, to, move, conflicts, sessions, tempDir, progress, permits)
            }
        }
    }

    private suspend fun viaTemp(
        s: Source.Remote,
        to: Place.Remote,
        move: Boolean,
        conflicts: ConflictPolicy,
        sessions: RemoteSessions,
        tempDir: File,
        progress: Progress,
        permits: Semaphore,
    ): String? {
        val tmp = File(tempDir, "transfer-${System.nanoTime()}").apply { mkdirs() }
        try {
            val local = File(tmp, s.name)
            downloadTree(s.id, s.path, s.isDirectory, local, sessions, Progress.SILENT, permits)
            val target = sessions.use(to.connectionId) { fs ->
                remoteTarget(fs, to.dir, s.name, s.isDirectory, s.size, s.modified, conflicts)
            } ?: return null
            uploadTree(to.connectionId, local, target, sessions, progress, permits)
            if (move) sessions.use(s.id) { fs -> fs.delete(s.path, recursive = s.isDirectory) }
            return RemoteUri.of(to.connectionId, target)
        } finally {
            FsUtil.deleteRecursively(tmp)
        }
    }

    private suspend fun remoteTarget(
        fs: RemoteFileSystem,
        dir: String,
        name: String,
        isDirectory: Boolean,
        srcSize: Long,
        srcModified: Long,
        conflicts: ConflictPolicy,
    ): String? {
        val entries = fs.list(dir)
        val existing = entries.firstOrNull { it.name == name } ?: return RemotePaths.join(dir, name)
        val policy = conflicts.resolve(name, isDirectory, srcSize, srcModified, true, existing.isDirectory, existing.size, existing.modified) ?: return null
        return when (policy) {
            Conflict.SKIP -> null
            Conflict.OVERWRITE, Conflict.ASK -> RemotePaths.join(dir, name)
            Conflict.KEEP_BOTH -> RemotePaths.join(dir, FsUtil.uniqueName(name, isDirectory) { candidate -> entries.any { it.name == candidate } })
        }
    }

    private suspend fun localTarget(
        dir: File,
        name: String,
        isDirectory: Boolean,
        srcSize: Long,
        srcModified: Long,
        conflicts: ConflictPolicy,
    ): File? {
        val target = File(dir, name)
        if (!target.exists()) return target
        val policy = conflicts.resolve(
            name, isDirectory, srcSize, srcModified,
            true, target.isDirectory, if (target.isDirectory) -1L else target.length(), target.lastModified(),
        ) ?: return null
        return when (policy) {
            Conflict.SKIP -> null
            Conflict.OVERWRITE, Conflict.ASK -> target.also { if (!isDirectory && it.isFile) it.delete() }
            Conflict.KEEP_BOTH -> FsUtil.uniqueSibling(target, isDirectory)
        }
    }

    private suspend fun uploadTree(
        connectionId: String,
        src: File,
        target: String,
        sessions: RemoteSessions,
        progress: Progress,
        permits: Semaphore,
    ) {
        if (!src.isDirectory) {
            permits.withPermit {
                sessions.useWorker(connectionId) { fs ->
                    BufferedInputStream(src.inputStream(), 1024 * 1024).use { input ->
                        fs.upload(input, target) { n -> progress.bytes(src.name, n) }
                    }
                }
            }
            progress.fileDone(src.name, src.length())
            return
        }
        // Create the directory skeleton and collect leaf files on the browsing connection (cheap metadata ops),
        // then push the leaves through dedicated worker connections in parallel.
        val leaves = ArrayList<Pair<File, String>>()
        sessions.use(connectionId) { fs ->
            fun walk(dir: File, remoteDir: String) {
                if (fs.stat(remoteDir)?.isDirectory != true) fs.mkdir(remoteDir)
                dir.listFiles()?.sortedBy { it.name }?.forEach { child ->
                    val childRemote = RemotePaths.join(remoteDir, child.name)
                    if (child.isDirectory) walk(child, childRemote) else leaves += child to childRemote
                }
            }
            walk(src, target)
        }
        coroutineScope {
            for ((file, remotePath) in leaves) {
                launch {
                    permits.withPermit {
                        sessions.useWorker(connectionId) { fs ->
                            BufferedInputStream(file.inputStream(), 1024 * 1024).use { input ->
                                fs.upload(input, remotePath) { n -> progress.bytes(file.name, n) }
                            }
                        }
                    }
                    progress.fileDone(file.name, file.length())
                }
            }
        }
    }

    private suspend fun downloadTree(
        connectionId: String,
        path: String,
        isDirectory: Boolean,
        target: File,
        sessions: RemoteSessions,
        progress: Progress,
        permits: Semaphore,
    ) {
        if (!isDirectory) {
            target.parentFile?.mkdirs()
            try {
                permits.withPermit {
                    sessions.useWorker(connectionId) { fs ->
                        BufferedOutputStream(target.outputStream(), 1024 * 1024).use { out ->
                            fs.download(path, out) { n -> progress.bytes(target.name, n) }
                        }
                    }
                }
            } catch (e: Exception) {
                target.delete()
                throw e
            }
            progress.fileDone(target.name, target.length())
            return
        }
        val leaves = ArrayList<Pair<String, File>>()
        sessions.use(connectionId) { fs ->
            fun walk(remoteDir: String, localDir: File) {
                localDir.mkdirs()
                fs.list(remoteDir).forEach { e ->
                    val childLocal = File(localDir, e.name)
                    if (e.isDirectory) walk(e.path, childLocal) else leaves += e.path to childLocal
                }
            }
            walk(path, target)
        }
        coroutineScope {
            for ((remotePath, file) in leaves) {
                launch {
                    file.parentFile?.mkdirs()
                    try {
                        permits.withPermit {
                            sessions.useWorker(connectionId) { fs ->
                                BufferedOutputStream(file.outputStream(), 1024 * 1024).use { out ->
                                    fs.download(remotePath, out) { n -> progress.bytes(file.name, n) }
                                }
                            }
                        }
                    } catch (e: Exception) {
                        file.delete()
                        throw e
                    }
                    progress.fileDone(file.name, file.length())
                }
            }
        }
    }

    private fun deleteLocal(file: File) {
        if (!FsUtil.deleteRecursively(file)) throw IOException(tr(R.string.remote_copied_not_removed))
    }

    private suspend fun measure(s: Source, sessions: RemoteSessions): Pair<Long, Int> = when (s) {
        is Source.Local -> if (s.file.isDirectory) {
            s.file.walkTopDown().filter { it.isFile }.fold(0L to 0) { (b, f), file -> (b + file.length()) to (f + 1) }
        } else {
            s.file.length() to 1
        }
        is Source.Remote -> if (s.isDirectory) sessions.use(s.id) { fs -> measureRemote(fs, s.path) } else s.size.coerceAtLeast(0) to 1
    }

    private fun measureRemote(fs: RemoteFileSystem, path: String): Pair<Long, Int> {
        var bytes = 0L
        var files = 0
        for (e in fs.list(path)) {
            if (e.isDirectory) {
                val (b, f) = measureRemote(fs, e.path)
                bytes += b
                files += f
            } else {
                bytes += e.size.coerceAtLeast(0)
                files++
            }
        }
        return bytes to files
    }

    /** Effective conflict policy with MT-style identical-file skipping and ask-once/apply-to-all caching. */
    private class ConflictPolicy(
        private val conflict: Conflict,
        private val resolver: ConflictResolver?,
        private val skipIdentical: Boolean,
    ) {
        private val lock = Mutex()
        private var appliedAll: Conflict? = null

        /** Returns the effective policy, or null when the existing entry is identical and should be skipped. */
        suspend fun resolve(
            name: String,
            isDirectory: Boolean,
            srcSize: Long,
            srcModified: Long,
            dstExists: Boolean,
            dstIsDirectory: Boolean,
            dstSize: Long,
            dstModified: Long,
        ): Conflict? {
            if (!dstExists) return conflict
            if (skipIdentical && !isDirectory && !dstIsDirectory && identicalFile(srcSize, srcModified, dstSize, dstModified)) return null
            return lock.withLock {
                var policy = appliedAll ?: conflict
                if (policy == Conflict.ASK) {
                    val decision = resolver?.resolve(ConflictInfo(name, isDirectory, srcSize, srcModified, dstSize, dstModified))
                        ?: ConflictDecision(Conflict.KEEP_BOTH)
                    if (decision.applyToAll) appliedAll = decision.conflict
                    policy = decision.conflict
                }
                policy
            }
        }
    }

    private class Results {
        private val lock = Any()
        val failed = ArrayList<String>()
        val skipped = ArrayList<String>()
        val created = ArrayList<String>()
        var okCount = 0
            private set

        fun ok(path: String) = synchronized(lock) { okCount++; created += path }
        fun skip(name: String) = synchronized(lock) { skipped += name }
        fun fail(message: String) = synchronized(lock) { failed += message }
    }

    private sealed interface Source {
        val name: String
        val isDirectory: Boolean

        data class Local(val file: File) : Source {
            override val name: String get() = file.name
            override val isDirectory: Boolean get() = file.isDirectory
        }

        data class Remote(val id: String, val path: String, override val isDirectory: Boolean, val size: Long, val modified: Long) : Source {
            override val name: String get() = RemotePaths.name(path)
        }

        companion object {
            fun of(item: FileItem): Source =
                RemoteUri.parse(item.path)?.let { (id, p) -> Remote(id, p, item.isDirectory, item.size, item.lastModified) } ?: Local(File(item.path))
        }
    }

    private class Progress(private val totalBytes: Long, private val totalFiles: Int, private val emit: (OpProgress) -> Unit) {
        private val lock = Any()
        private var doneBytes = 0L
        private var doneFiles = 0
        private var lastReported = 0L

        fun bytes(name: String, inFile: Long) {
            synchronized(lock) {
                if (inFile - lastReported < 256 * 1024) return
                lastReported = inFile
                emit(OpProgress.Running(name, doneBytes + inFile, totalBytes, doneFiles, totalFiles))
            }
        }

        fun fileDone(name: String, size: Long) {
            synchronized(lock) {
                doneBytes += size.coerceAtLeast(0)
                doneFiles++
                lastReported = 0
                emit(OpProgress.Running(name, doneBytes, totalBytes, doneFiles, totalFiles))
            }
        }

        companion object {
            val SILENT = Progress(0, 0) { }
        }
    }
}
