package dev.cluvex.strata.feature.explorer.components

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Checkbox
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import dev.cluvex.strata.R
import dev.cluvex.strata.core.fs.Conflict
import dev.cluvex.strata.core.fs.ConflictInfo

@Composable
fun TransferDialog(
    move: Boolean,
    count: Int,
    destination: String,
    conflicts: List<String> = emptyList(),
    onConfirm: (Conflict) -> Unit,
    onDismiss: () -> Unit,
) {
    var conflict by remember { mutableStateOf(Conflict.KEEP_BOTH) }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(R.string.explorer_name_conflict)) },
        text = {
            Column {
                val n = conflicts.size
                val header = if (n == 1) {
                    stringResource(R.string.explorer_conflict_one, conflicts.first())
                } else {
                    stringResource(R.string.explorer_conflict_many, n, count)
                }
                Text(header)
                if (n > 1) {
                    val list = conflicts.take(6).joinToString("\n") { "• $it" } + if (n > 6) "\n• …" else ""
                    Text(
                        list,
                        fontFamily = FontFamily.Monospace,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(top = 4.dp),
                    )
                }
                Text(
                    destination,
                    fontFamily = FontFamily.Monospace,
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.padding(top = 8.dp, bottom = 12.dp),
                )
                Text(stringResource(R.string.explorer_conflict_question), style = MaterialTheme.typography.labelLarge)
                Column(Modifier.selectableGroup()) {
                    listOf(
                        Conflict.KEEP_BOTH to stringResource(R.string.explorer_conflict_keep_both),
                        Conflict.OVERWRITE to stringResource(R.string.explorer_conflict_replace),
                        Conflict.SKIP to stringResource(R.string.common_skip),
                        Conflict.ASK to stringResource(R.string.explorer_conflict_ask_each),
                    ).forEach { (option, label) ->
                        Row(
                            Modifier
                                .fillMaxWidth()
                                .selectable(selected = conflict == option, onClick = { conflict = option }, role = Role.RadioButton)
                                .padding(vertical = 6.dp),
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            RadioButton(selected = conflict == option, onClick = null)
                            Text(label, modifier = Modifier.padding(start = 8.dp))
                        }
                    }
                }
                Text(
                    stringResource(R.string.explorer_conflict_identical_note),
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(top = 8.dp),
                )
            }
        },
        confirmButton = { TextButton(onClick = { onConfirm(conflict) }) { Text(stringResource(if (move) R.string.common_move else R.string.common_copy)) } },
        dismissButton = { TextButton(onClick = onDismiss) { Text(stringResource(R.string.common_cancel)) } },
    )
}

/** MT-style per-file conflict prompt with size/date comparison and apply-to-all. */
@Composable
fun FileConflictDialog(
    info: ConflictInfo,
    onDecision: (Conflict, Boolean) -> Unit,
    onDismiss: () -> Unit,
) {
    var applyAll by remember { mutableStateOf(false) }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(R.string.file_conflict_title)) },
        text = {
            Column {
                Text(info.name, style = MaterialTheme.typography.titleMedium)
                Text(
                    stringResource(R.string.file_conflict_source, formatConflictSize(info.srcSize), formatConflictTime(info.srcModified)),
                    style = MaterialTheme.typography.bodyMedium,
                    modifier = Modifier.padding(top = 8.dp),
                )
                Text(
                    stringResource(R.string.file_conflict_destination, formatConflictSize(info.dstSize), formatConflictTime(info.dstModified)),
                    style = MaterialTheme.typography.bodyMedium,
                    modifier = Modifier.padding(top = 4.dp),
                )
                Row(
                    Modifier.fillMaxWidth().padding(top = 8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Checkbox(checked = applyAll, onCheckedChange = { applyAll = it })
                    Text(stringResource(R.string.file_conflict_apply_all), modifier = Modifier.padding(start = 8.dp))
                }
            }
        },
        confirmButton = {
            Row {
                TextButton(onClick = { onDecision(Conflict.SKIP, applyAll) }) { Text(stringResource(R.string.common_skip)) }
                TextButton(onClick = { onDecision(Conflict.KEEP_BOTH, applyAll) }) { Text(stringResource(R.string.file_conflict_keep_both)) }
                TextButton(onClick = { onDecision(Conflict.OVERWRITE, applyAll) }) { Text(stringResource(R.string.file_conflict_overwrite)) }
            }
        },
    )
}

private fun formatConflictSize(bytes: Long): String = when {
    bytes < 0 -> "—"
    bytes < 1024 -> "$bytes B"
    bytes < 1024 * 1024 -> String.format(java.util.Locale.US, "%.1f KB", bytes / 1024.0)
    bytes < 1024L * 1024 * 1024 -> String.format(java.util.Locale.US, "%.1f MB", bytes / (1024.0 * 1024))
    else -> String.format(java.util.Locale.US, "%.2f GB", bytes / (1024.0 * 1024 * 1024))
}

private fun formatConflictTime(millis: Long): String =
    if (millis <= 0) "—"
    else java.text.DateFormat.getDateTimeInstance(java.text.DateFormat.MEDIUM, java.text.DateFormat.SHORT).format(java.util.Date(millis))
