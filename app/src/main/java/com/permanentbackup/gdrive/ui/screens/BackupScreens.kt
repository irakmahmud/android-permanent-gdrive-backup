package com.permanentbackup.gdrive.ui.screens

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

enum class AppScreen(val title: String) {
    DASHBOARD("Dashboard"),
    ACCOUNT("Account"),
    FOLDERS("Folders"),
    SETTINGS("Settings"),
    ACTIVITY("Activity"),
    HISTORY("History"),
    FAILED("Failed")
}

@Composable
fun DashboardScreen(
    summary: DashboardSummary,
    settings: BackupSettings,
    onBackupNow: () -> Unit,
    onOpenAccountScreen: () -> Unit,
    onOpenFoldersScreen: () -> Unit,
    onEnableConsentAndPermissions: () -> Unit,
    onRefresh: () -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        // Prominent Quick Setup Card right at the top of Dashboard!
        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.secondaryContainer
                ),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(
                    modifier = Modifier.padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text(
                        text = "Quick Setup (গুগল ড্রাইভ ও ফোল্ডার পারমিশন)",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    Button(
                        onClick = onOpenAccountScreen,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text(
                            if (summary.accountState.isConnected) {
                                "✓ Google Account Connected (${summary.accountState.email}) — Manage"
                            } else {
                                "১. গুগল ড্রাইভ একাউন্ট যুক্ত করুন (Connect Google Account)"
                            }
                        )
                    }
                    Button(
                        onClick = onEnableConsentAndPermissions,
                        modifier = Modifier.fillMaxWidth(),
                        colors = ButtonDefaults.buttonColors(
                            containerColor = if (settings.userBackupConsentGranted && summary.accessibleFolderCount > 0) {
                                MaterialTheme.colorScheme.secondary
                            } else {
                                MaterialTheme.colorScheme.primary
                        }
                        )
                    ) {
                        Text(
                            if (settings.userBackupConsentGranted && summary.accessibleFolderCount > 0) {
                                "✓ স্টোরেজ পারমিশন সক্রিয় (${summary.accessibleFolderCount} Folders)"
                            } else {
                                "২. ফোল্ডার ও স্টোরেজ পারমিশন দিন (Grant Storage Access)"
                            }
                        )
                    }
                    OutlinedButton(
                        onClick = onOpenFoldersScreen,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text("ফোল্ডার তালিকা দেখুন (View Accessible & Restricted Folders)")
                    }
                }
            }
        }

        item {
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = MaterialTheme.colorScheme.primaryContainer
                ),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(modifier = Modifier.padding(16.dp)) {
                    Text(
                        text = "STRICT NEVER-DELETE POLICY ACTIVE",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        text = "Upload-Only & Append-Only to Google Drive ('Android_Permanent_Backup'). Local file deletions, moves, or modifications NEVER delete or overwrite cloud backups.",
                        style = MaterialTheme.typography.bodySmall
                    )
                }
            }
        }

        // Alert Banners for Permission Failure, Quota Full, or Network Failure
        if (summary.permissionFailureMessage != null) {
            item {
                AlertBanner(
                    title = "Permission Required",
                    message = summary.permissionFailureMessage,
                    actionLabel = "Grant Storage Access",
                    onAction = onEnableConsentAndPermissions
                )
            }
        }

        if (summary.quotaFullAlert || summary.driveQuota.isQuotaFull) {
            item {
                AlertBanner(
                    title = "Google Drive Storage Quota Full",
                    message = "Cloud storage limit reached. Existing backups are safe; free up Drive space to resume new uploads."
                )
            }
        }

        if (summary.networkFailureMessage != null) {
            item {
                AlertBanner(
                    title = "Network / Upload Notice",
                    message = summary.networkFailureMessage
                )
            }
        }

        // Currently Uploading File Card
        summary.currentlyUploading?.let { active ->
            item {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(16.dp)) {
                        Text(
                            text = "Currently Streaming Upload (64 KiB RAM Buffer)",
                            fontWeight = FontWeight.Bold
                        )
                        Text("File: ${active.fileName} (v${active.backupVersion})")
                        Text("Path: ${active.relativePath}")
                        Text("Progress: ${formatBytes(active.uploadedBytes)} / ${formatBytes(active.totalBytes)}")
                        Spacer(modifier = Modifier.height(6.dp))
                        LinearProgressIndicator(
                            progress = { active.fraction },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                }
            }
        }

        // Main Metrics Card
        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    Text(
                        text = "Backup Status Overview",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    MetricRow(
                        "Google Account",
                        if (summary.accountState.isConnected) {
                            "Connected (${summary.accountState.email})"
                        } else {
                            "Disconnected"
                        }
                    )
                    MetricRow("Root Drive Folder", summary.accountState.rootDriveFolderName)
                    MetricRow("Total Discovered Versions", summary.totalDiscoveredFiles.toString())
                    MetricRow("Successfully Backed Up", summary.successfullyBackedUpFiles.toString())
                    MetricRow("Pending / Queued Files", summary.pendingFiles.toString())
                    MetricRow("Failed Files", summary.failedFiles.toString())
                    MetricRow(
                        "Preserved on Drive After Phone Delete",
                        summary.preservedAfterLocalDeleteCount.toString()
                    )
                    MetricRow("Total Uploaded Data", formatBytes(summary.totalUploadedBytes))
                    MetricRow(
                        "Overall Backup Progress",
                        "${(summary.overallProgressFraction * 100).toInt()}%"
                    )
                    LinearProgressIndicator(
                        progress = { summary.overallProgressFraction },
                        modifier = Modifier.fillMaxWidth()
                    )
                    MetricRow("Last Successful Backup", formatTimestamp(summary.lastSuccessfulBackupMs))
                    MetricRow("Next Scheduled Backup", formatTimestamp(summary.nextScheduledBackupMs))
                    MetricRow(
                        "Available Google Drive Storage",
                        "${formatBytes(summary.driveQuota.availableBytes)} free of ${formatBytes(summary.driveQuota.limitBytes)}"
                    )
                    MetricRow(
                        "Storage Access Coverage",
                        "${summary.accessibleFolderCount} accessible / ${summary.restrictedFolderCount} OS-restricted"
                    )
                }
            }
        }

        item {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                Button(
                    onClick = onBackupNow,
                    modifier = Modifier.weight(1f)
                ) {
                    Text(if (summary.isBackupRunning) "Backup Running..." else "Manual Backup Now")
                }
                OutlinedButton(
                    onClick = onRefresh,
                    modifier = Modifier.weight(1f)
                ) {
                    Text("Refresh Status")
                }
            }
        }
    }
}

@Composable
fun BackupActivityScreen(
    activeQueue: List<BackupFileEntity>,
    onTriggerBackup: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Active & Pending Upload Queue (${activeQueue.size})",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
            Button(onClick = onTriggerBackup) {
                Text("Run Queue")
            }
        }
        Spacer(modifier = Modifier.height(12.dp))
        if (activeQueue.isEmpty()) {
            Text("No active or pending uploads in queue. All discovered files are up to date.")
        } else {
            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(activeQueue, key = { it.id }) { item ->
                    Card(modifier = Modifier.fillMaxWidth()) {
                        Column(modifier = Modifier.padding(12.dp)) {
                            Text("${item.driveStoredName} (v${item.backupVersion})", fontWeight = FontWeight.Bold)
                            Text("Relative Path: Android_Permanent_Backup/${item.relativePath}")
                            Text("Status: ${item.uploadStatus} | Resumed Offset: ${formatBytes(item.uploadedBytes)} / ${formatBytes(item.fileSize)}")
                            if (!item.resumableSessionUri.isNullOrBlank()) {
                                Text("Resumable Session Saved: Active (Chunk Resume Ready)")
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
fun BackupHistoryScreen(verifiedHistory: List<BackupFileEntity>) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        Text(
            text = "Permanent Cloud Backup History (${verifiedHistory.size})",
            style = MaterialTheme.typography.titleMedium,
            fontWeight = FontWeight.Bold
        )
        Text(
            text = "Immutable append-only record of verified Google Drive backups. Files deleted from phone remain preserved in Google Drive.",
            style = MaterialTheme.typography.bodySmall
        )
        Spacer(modifier = Modifier.height(12.dp))
        LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            items(verifiedHistory, key = { it.id }) { item ->
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(12.dp)) {
                        Text("${item.driveStoredName} (Version ${item.backupVersion})", fontWeight = FontWeight.Bold)
                        Text("Drive File ID: ${item.driveFileId ?: "N/A"}")
                        Text("Cloud Path: Android_Permanent_Backup/${item.relativePath}")
                        Text("Size: ${formatBytes(item.fileSize)} | Integrity: ${item.verificationStatus}")
                        Text("SHA-256: ${item.sha256Hash?.take(20) ?: "Stream Verified"}...")
                        Text(
                            text = if (item.localExists) {
                                "Phone Status: Present on phone & backed up on Drive"
                            } else {
                                "Phone Status: Deleted on phone — PERMANENTLY PRESERVED ON GOOGLE DRIVE"
                            },
                            fontWeight = FontWeight.SemiBold
                        )
                    }
                }
            }
        }
    }
}

@Composable
fun FailedUploadsScreen(
    failedList: List<BackupFileEntity>,
    onRetryAll: () -> Unit,
    onRetrySingle: (Long) -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Failed / Interrupted Uploads (${failedList.size})",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
            Button(onClick = onRetryAll) {
                Text("Retry All")
            }
        }
        Spacer(modifier = Modifier.height(12.dp))
        if (failedList.isEmpty()) {
            Text("Zero failed uploads.")
        } else {
            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(failedList, key = { it.id }) { item ->
                    Card(modifier = Modifier.fillMaxWidth()) {
                        Column(modifier = Modifier.padding(12.dp)) {
                            Text("${item.driveStoredName} (v${item.backupVersion})", fontWeight = FontWeight.Bold)
                            Text("Reason: ${item.failureReason}")
                            if (!item.failureDetail.isNullOrBlank()) {
                                Text("Detail: ${item.failureDetail}")
                            }
                            Text("Saved Resume Offset: ${formatBytes(item.uploadedBytes)} / ${formatBytes(item.fileSize)} (Retries: ${item.retryCount})")
                            Spacer(modifier = Modifier.height(6.dp))
                            OutlinedButton(onClick = { onRetrySingle(item.id) }) {
                                Text("Resume / Retry Upload")
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
fun FolderCoverageScreen(
    coverageItems: List<FolderCoverageItem>,
    onGrantAllFilesAccess: () -> Unit,
    onSelectSafTree: () -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            Text(
                text = "Storage & Folder Coverage",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
            Text(
                text = "Accessible folders are automatically scanned and backed up into 'Android_Permanent_Backup'. Android OS-restricted app-private folders are listed separately and never falsely claimed as backed up.",
                style = MaterialTheme.typography.bodySmall
            )
            Spacer(modifier = Modifier.height(8.dp))
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = onGrantAllFilesAccess) {
                    Text("Grant All-Files Permission")
                }
                OutlinedButton(onClick = onSelectSafTree) {
                    Text("Add SAF Folder")
                }
            }
        }

        items(coverageItems, key = { it.folderName + it.relativePath }) { item ->
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Text(item.folderName, fontWeight = FontWeight.Bold)
                    Text("Path: ${item.relativePath}")
                    Text("Access Status: ${item.accessStatus} | Discovered Files: ${item.discoveredFileCount}")
                    Text(
                        text = item.restrictionExplanation,
                        style = MaterialTheme.typography.bodySmall
                    )
                    if (item.accessStatus == FolderAccessStatus.RESTRICTED_BY_ANDROID_OS) {
                        Text(
                            text = "Note: Blocked by Android OS sandbox security; excluded from backup claims.",
                            color = MaterialTheme.colorScheme.error,
                            style = MaterialTheme.typography.bodySmall
                        )
                    }
                }
            }
        }
    }
}

@Composable
fun SettingsScreen(
    settings: BackupSettings,
    onUpdateSettings: (BackupSettings) -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            Text(
                text = "Backup Engine & Scheduling Settings",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
        }

        item {
            SettingToggleRow(
                title = "User Backup Consent (Required to Read Local Files)",
                subtitle = "App will never read or upload any file without explicit consent.",
                checked = settings.userBackupConsentGranted,
                onCheckedChange = { onUpdateSettings(settings.copy(userBackupConsentGranted = it)) }
            )
        }

        item {
            SettingToggleRow(
                title = "Wi-Fi / Unmetered Network Only",
                subtitle = "Pause background uploads on metered mobile data.",
                checked = settings.wifiOnly,
                onCheckedChange = {
                    onUpdateSettings(settings.copy(wifiOnly = it, unmeteredNetworkOnly = it))
                }
            )
        }

        item {
            SettingToggleRow(
                title = "Charging Only",
                subtitle = "Run background WorkManager backups only while phone is charging.",
                checked = settings.chargingOnly,
                onCheckedChange = { onUpdateSettings(settings.copy(chargingOnly = it)) }
            )
        }

        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Text("Streaming Resumable Upload Configuration", fontWeight = FontWeight.Bold)
                    Text("Chunk Size: ${formatBytes(settings.chunkSizeBytes.toLong())} (64 KiB RAM buffer, Zero temp file copies)")
                    Text("Upload Concurrency: ${settings.uploadConcurrency} stream(s)")
                    Text("Periodic Background Scan Interval: Every ${settings.periodicIntervalHours} hours")
                    Text("Remote Deletion Policy: PERMANENTLY DISABLED (Strictly Append-Only)")
                }
            }
        }
    }
}

@Composable
fun GoogleAccountScreen(
    summary: DashboardSummary,
    signInFeedbackMessage: String? = null,
    onLaunchGoogleSignIn: () -> Unit,
    onLaunchBrowserOAuth: () -> Unit = {},
    onConnectTokenManually: (String, String, String, String?) -> Unit,
    onSwitchAccount: (String, String, String, String?) -> Unit,
    onDisconnect: () -> Unit
) {
    var emailInput by remember { mutableStateOf(summary.accountState.email) }
    var nameInput by remember { mutableStateOf(summary.accountState.displayName) }
    var accessTokenInput by remember { mutableStateOf("") }
    var refreshTokenInput by remember { mutableStateOf("") }

    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    Text(
                        text = "Google Drive OAuth 2.0 Connection (গুগল ড্রাইভ একাউন্ট)",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    Text("Status: ${if (summary.accountState.isConnected) "Connected ✓" else "Disconnected"}")
                    Text("Account Email: ${summary.accountState.email.ifBlank { "Not signed in" }}")
                    Text("Display Name: ${summary.accountState.displayName.ifBlank { "N/A" }}")
                    Text(
                        "Available Drive Storage: ${formatBytes(summary.driveQuota.availableBytes)} free of ${formatBytes(summary.driveQuota.limitBytes)}"
                    )
                    Text("Root Backup Folder: ${summary.accountState.rootDriveFolderName}")
                    Text("Root Folder ID: ${summary.accountState.rootDriveFolderId ?: "Will be created on first upload"}")
                    summary.accountState.connectionError?.let { err ->
                        Text("Notice: $err", color = MaterialTheme.colorScheme.error)
                    }
                    signInFeedbackMessage?.let { msg ->
                        Text(msg, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.SemiBold)
                    }
                    Spacer(modifier = Modifier.height(8.dp))
                    Button(
                        onClick = onLaunchGoogleSignIn,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text(
                            if (summary.accountState.isConnected) {
                                "✓ Re-Verify & Refresh Google Drive Token (1-Tap)"
                            } else {
                                "Sign in with Google (1-Tap Auto-Connect)"
                            }
                        )
                    }
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        OutlinedButton(
                            onClick = onLaunchBrowserOAuth,
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Browser Sign-In (অন্য একাউন্ট)")
                        }
                        if (summary.accountState.isConnected) {
                            OutlinedButton(
                                onClick = onDisconnect,
                                modifier = Modifier.weight(1f)
                            ) {
                                Text("Disconnect")
                            }
                        }
                    }
                }
            }
        }

        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text(
                        "Direct Google Drive Account & OAuth Token Setup",
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        "অন্য কোনো কাস্টম Google একাউন্ট ও OAuth 2.0 Token সরাসরি দিতে চাইলে নিচের ঘরগুলো পূরণ করে 'Authorize Account' চাপুন:",
                        style = MaterialTheme.typography.bodySmall
                    )
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Button(
                            onClick = {
                                if (emailInput.isNotBlank() && (accessTokenInput.isNotBlank() || refreshTokenInput.isNotBlank())) {
                                    onConnectTokenManually(
                                        emailInput.trim(),
                                        nameInput.trim().ifBlank { emailInput.trim() },
                                        accessTokenInput.trim(),
                                        refreshTokenInput.trim().ifBlank { null }
                                    )
                                }
                            },
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Authorize Account")
                        }
                        OutlinedButton(
                            onClick = {
                                if (emailInput.isNotBlank() && (accessTokenInput.isNotBlank() || refreshTokenInput.isNotBlank())) {
                                    onSwitchAccount(
                                        emailInput.trim(),
                                        nameInput.trim().ifBlank { emailInput.trim() },
                                        accessTokenInput.trim(),
                                        refreshTokenInput.trim().ifBlank { null }
                                    )
                                }
                            },
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Switch Account")
                        }
                    }
                    OutlinedTextField(
                        value = emailInput,
                        onValueChange = { emailInput = it },
                        label = { Text("Google Account Email (যেমন: yourname@gmail.com)") },
                        modifier = Modifier.fillMaxWidth()
                    )
                    OutlinedTextField(
                        value = nameInput,
                        onValueChange = { nameInput = it },
                        label = { Text("Display Name (ঐচ্ছিক)") },
                        modifier = Modifier.fillMaxWidth()
                    )
                    OutlinedTextField(
                        value = accessTokenInput,
                        onValueChange = { accessTokenInput = it },
                        label = { Text("OAuth 2.0 Access Token (ya29...)") },
                        modifier = Modifier.fillMaxWidth()
                    )
                    OutlinedTextField(
                        value = refreshTokenInput,
                        onValueChange = { refreshTokenInput = it },
                        label = { Text("OAuth 2.0 Refresh Token (1//... স্বয়ংক্রিয় রিফ্রেশের জন্য)") },
                        modifier = Modifier.fillMaxWidth()
                    )
                }
            }
        }
    }
}

@Composable
private fun AlertBanner(
    title: String,
    message: String,
    actionLabel: String? = null,
    onAction: (() -> Unit)? = null
) {
    Card(
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.errorContainer),
        modifier = Modifier.fillMaxWidth()
    ) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(title, fontWeight = FontWeight.Bold, color = MaterialTheme.colorScheme.onErrorContainer)
            Text(message, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onErrorContainer)
            if (actionLabel != null && onAction != null) {
                Spacer(modifier = Modifier.height(6.dp))
                Button(onClick = onAction) {
                    Text(actionLabel)
                }
            }
        }
    }
}

@Composable
private fun MetricRow(label: String, value: String) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Text(text = label, style = MaterialTheme.typography.bodyMedium)
        Text(text = value, style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
    }
}

@Composable
private fun SettingToggleRow(
    title: String,
    subtitle: String,
    checked: Boolean,
    onCheckedChange: (Boolean) -> Unit
) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(12.dp),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Column(modifier = Modifier.weight(1f)) {
                Text(title, fontWeight = FontWeight.Bold)
                Text(subtitle, style = MaterialTheme.typography.bodySmall)
            }
            Switch(checked = checked, onCheckedChange = onCheckedChange)
        }
    }
}

private fun formatBytes(bytes: Long): String {
    if (bytes <= 0L) return "0 B"
    val kb = 1024.0
    val mb = kb * 1024.0
    val gb = mb * 1024.0
    return when {
        bytes >= gb -> String.format(Locale.US, "%.2f GB", bytes / gb)
        bytes >= mb -> String.format(Locale.US, "%.2f MB", bytes / mb)
        bytes >= kb -> String.format(Locale.US, "%.1f KB", bytes / kb)
        else -> "$bytes B"
    }
}

private fun formatTimestamp(epochMs: Long?): String {
    if (epochMs == null || epochMs <= 0L) return "Never"
    val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US)
    return sdf.format(Date(epochMs))
}
