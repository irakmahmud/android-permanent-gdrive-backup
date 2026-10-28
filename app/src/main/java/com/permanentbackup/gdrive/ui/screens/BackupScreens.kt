package com.permanentbackup.gdrive.ui.screens

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.FilterChip
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.GoogleAccountEntity
import com.permanentbackup.gdrive.data.local.SyncEventEntity
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.DriveSortOrder
import com.permanentbackup.gdrive.domain.model.DriveViewFilter
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.LocalBrowseItem
import com.permanentbackup.gdrive.domain.model.OAuthDiagnosticReport
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Section 12: All 9 Mandatory Main Navigation Destinations for the Google Drive Desktop-Class Client.
 */
enum class AppScreen(val title: String) {
    HOME("Home"),
    MY_DRIVE("My Drive"),
    PHONE_STORAGE("Phone"),
    BACKUPS("Backups"),
    RECENT("Recent"),
    TRANSFERS("Transfers"),
    OFFLINE("Offline"),
    SETTINGS("Settings"),
    ACCOUNTS("Accounts")
}

@Composable
fun HomeDashboardScreen(
    summary: DashboardSummary,
    settings: BackupSettings,
    actionFeedback: String?,
    onBackupNow: () -> Unit,
    onConnectOrManageAccount: () -> Unit,
    onOpenPhoneStorageScreen: () -> Unit,
    onOpenMyDriveScreen: () -> Unit,
    onEnableConsentAndPermissions: () -> Unit,
    onRefresh: () -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        actionFeedback?.let { msg ->
            item {
                Card(
                    colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.tertiaryContainer),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Text(
                        text = msg,
                        modifier = Modifier.padding(12.dp),
                        style = MaterialTheme.typography.bodySmall,
                        fontWeight = FontWeight.SemiBold
                    )
                }
            }
        }

        // Quick Setup & Persistent Connection Card
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
                        text = "Permanent Drive — Desktop-Class Android Client",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    Button(
                        onClick = onConnectOrManageAccount,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text(
                            if (summary.accountState.isConnected) {
                                "✓ Connected: ${summary.accountState.email} (Persistent Session)"
                            } else {
                                "1. Connect Google Account (গুগল ড্রাইভ যুক্ত করুন)"
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
                                "✓ Phone Storage Access Active (${summary.accessibleFolderCount} Folders)"
                            } else {
                                "2. Grant Phone Storage Permission (স্টোরেজ পারমিশন দিন)"
                            }
                        )
                    }
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        OutlinedButton(
                            onClick = onOpenMyDriveScreen,
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Explore My Drive")
                        }
                        OutlinedButton(
                            onClick = onOpenPhoneStorageScreen,
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Phone Explorer")
                        }
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
                        text = "IMMUTABLE NEVER-DELETE POLICY ACTIVE (3-Layer Guard)",
                        style = MaterialTheme.typography.titleSmall,
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        text = "Append-Only & Non-Destructive Backup to '${summary.accountState.rootDriveFolderName}'. Local file delete, folder delete, rename, move, or modification NEVER deletes or overwrites previous cloud versions.",
                        style = MaterialTheme.typography.bodySmall
                    )
                }
            }
        }

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
                    title = "Google Drive Storage Full",
                    message = "Cloud storage limit reached. Existing backups remain 100% intact; free up Drive space to resume new uploads."
                )
            }
        }

        if (summary.networkFailureMessage != null) {
            item {
                AlertBanner(
                    title = "Sync / Network Notice",
                    message = summary.networkFailureMessage
                )
            }
        }

        summary.currentlyUploading?.let { active ->
            item {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(16.dp)) {
                        Text(
                            text = "Streaming Upload (${active.syncJobState.name})",
                            fontWeight = FontWeight.Bold
                        )
                        Text("File: ${active.fileName} (v${active.backupVersion})")
                        Text("Progress: ${formatBytes(active.uploadedBytes)} / ${formatBytes(active.totalBytes)} (${(active.fraction * 100).toInt()}%)")
                        Text(
                            "Speed: ${formatBytes(active.transferSpeedBps)}/s | ETA: ${
                                active.estimatedRemainingSeconds?.let { "${it}s" } ?: "Calculating..."
                            }"
                        )
                        Spacer(modifier = Modifier.height(6.dp))
                        LinearProgressIndicator(
                            progress = { active.fraction },
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                }
            }
        }

        // Section 12 Home Dashboard Metrics
        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    Text(
                        text = "Home Dashboard Overview",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    MetricRow(
                        "Connected Google Account",
                        if (summary.accountState.isConnected) summary.accountState.email else "Disconnected"
                    )
                    MetricRow("Background Sync Status", summary.backgroundSyncStatus.label)
                    MetricRow(
                        "Google Drive Storage Used",
                        formatBytes(summary.driveQuota.usageBytes)
                    )
                    MetricRow(
                        "Google Drive Storage Available",
                        "${formatBytes(summary.driveQuota.availableBytes)} of ${formatBytes(summary.driveQuota.limitBytes)}"
                    )
                    MetricRow("Last Successful Sync", formatTimestamp(summary.lastSuccessfulBackupMs))
                    MetricRow("Current Upload Speed", "${formatBytes(summary.currentUploadSpeedBps)}/s")
                    MetricRow("Total Uploaded Data", formatBytes(summary.totalUploadedBytes))
                    MetricRow("Successfully Backed Up Versions", summary.successfullyBackedUpFiles.toString())
                    MetricRow("Pending / Queued Files", summary.pendingFiles.toString())
                    MetricRow("Failed Files", summary.failedFiles.toString())
                    MetricRow(
                        "Preserved After Local Phone Delete",
                        summary.preservedAfterLocalDeleteCount.toString()
                    )
                    MetricRow(
                        "Backup Coverage",
                        "${summary.accessibleFolderCount} accessible folders (${(summary.overallProgressFraction * 100).toInt()}%)"
                    )
                    LinearProgressIndicator(
                        progress = { summary.overallProgressFraction },
                        modifier = Modifier.fillMaxWidth()
                    )
                    MetricRow(
                        "Offline Cache Usage",
                        "${formatBytes(summary.offlineCacheUsedBytes)} / ${formatBytes(summary.offlineCacheBudgetBytes)} (${summary.offlinePinnedCount} pinned)"
                    )
                    MetricRow(
                        "SQLite WAL Integrity",
                        if (summary.dbIntegrityOk) "Verified OK (12 Entities)" else "Check Required"
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
                    Text(if (summary.isBackupRunning) "Backup Running..." else "Backup Now")
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

/**
 * Section 5A: My Drive Explorer Screen
 * Supports Folder Navigation, List/Grid View, Search, Sort, Metadata, Preview, Download,
 * Open With Supported Apps (Streaming ContentProvider), Share Link, Recent, Starred, Shared, Shared Drives.
 */
@Composable
fun MyDriveExplorerScreen(
    accountConnected: Boolean,
    hasFullDriveReadScope: Boolean,
    items: List<CloudBrowseItem>,
    currentFilter: DriveViewFilter,
    currentSort: DriveSortOrder,
    breadcrumbs: List<Pair<String, String>>,
    searchQuery: String,
    isGridView: Boolean,
    previewSnippet: Pair<String, String>?,
    onSelectFilter: (DriveViewFilter) -> Unit,
    onSelectSort: (DriveSortOrder) -> Unit,
    onSearchQueryChange: (String) -> Unit,
    onToggleGridView: () -> Unit,
    onOpenFolder: (String, String) -> Unit,
    onNavigateBreadcrumb: (Int) -> Unit,
    onPreviewFile: (CloudBrowseItem) -> Unit,
    onDismissPreview: () -> Unit,
    onStreamOpenExternal: (CloudBrowseItem) -> Unit,
    onDownloadRestoreFile: (CloudBrowseItem) -> Unit,
    onTogglePinOffline: (CloudBrowseItem) -> Unit,
    onShareFileLink: (CloudBrowseItem) -> Unit,
    onRequestFullDriveReadScope: () -> Unit,
    onRefresh: () -> Unit
) {
    var selectedMetadataItem by remember { mutableStateOf<CloudBrowseItem?>(null) }

    if (previewSnippet != null) {
        AlertDialog(
            onDismissRequest = onDismissPreview,
            title = { Text("Cloud Stream Preview: ${previewSnippet.first}") },
            text = {
                Text(
                    text = previewSnippet.second.take(2000),
                    style = MaterialTheme.typography.bodySmall
                )
            },
            confirmButton = {
                TextButton(onClick = onDismissPreview) {
                    Text("Close")
                }
            }
        )
    }

    selectedMetadataItem?.let { meta ->
        AlertDialog(
            onDismissRequest = { selectedMetadataItem = null },
            title = { Text(meta.name, fontWeight = FontWeight.Bold) },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Text("Drive File ID: ${meta.driveFileId}")
                    Text("MIME Type: ${meta.mimeType}")
                    Text("File Size: ${formatBytes(meta.sizeBytes)}")
                    Text("Created: ${formatTimestamp(meta.createdTimeMs)}")
                    Text("Modified: ${formatTimestamp(meta.modifiedTimeMs)}")
                    Text("SHA-256: ${meta.sha256Checksum ?: "N/A"}")
                    Text("MD5: ${meta.md5Checksum ?: "N/A"}")
                    Text("Protected Backup File: ${if (meta.isProtectedBackupFile) "Yes (Never-Delete Protected)" else "Standard Drive File"}")
                    Text("Starred: ${meta.starred} | Shared: ${meta.shared}")
                }
            },
            confirmButton = {
                TextButton(onClick = { selectedMetadataItem = null }) {
                    Text("Close")
                }
            }
        )
    }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        // Scope Transparency Banner (Section 5A)
        if (accountConnected && !hasFullDriveReadScope) {
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.secondaryContainer),
                modifier = Modifier.fillMaxWidth()
            ) {
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(10.dp),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Column(modifier = Modifier.weight(1f)) {
                        Text(
                            "Least-Privilege Scope (drive.file) Active",
                            fontWeight = FontWeight.Bold,
                            fontSize = 12.sp
                        )
                        Text(
                            "Showing files inside 'Android_Permanent_Backup'. Grant 'drive.readonly' to browse all non-app My Drive files.",
                            style = MaterialTheme.typography.bodySmall,
                            fontSize = 11.sp
                        )
                    }
                    OutlinedButton(onClick = onRequestFullDriveReadScope) {
                        Text("Grant Full View", fontSize = 11.sp)
                    }
                }
            }
        }

        // Search + Grid/List + Refresh Row
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(6.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            OutlinedTextField(
                value = searchQuery,
                onValueChange = onSearchQueryChange,
                label = { Text("Search Google Drive...") },
                singleLine = true,
                modifier = Modifier.weight(1f)
            )
            OutlinedButton(onClick = onToggleGridView) {
                Text(if (isGridView) "List" else "Grid")
            }
            OutlinedButton(onClick = onRefresh) {
                Text("Sync")
            }
        }

        // Filter Chips (Backup Root, My Drive, Recent, Starred, Shared, Shared Drives)
        LazyRow(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
            items(DriveViewFilter.entries) { filter ->
                FilterChip(
                    selected = currentFilter == filter,
                    onClick = { onSelectFilter(filter) },
                    label = { Text(filter.title, fontSize = 12.sp) }
                )
            }
        }

        // Sort Chips
        LazyRow(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
            items(DriveSortOrder.entries) { sort ->
                FilterChip(
                    selected = currentSort == sort,
                    onClick = { onSelectSort(sort) },
                    label = { Text("Sort: ${sort.title}", fontSize = 11.sp) }
                )
            }
        }

        // Folder Breadcrumbs
        if (breadcrumbs.isNotEmpty()) {
            LazyRow(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                item {
                    TextButton(onClick = { onNavigateBreadcrumb(-1) }) {
                        Text("Root /")
                    }
                }
                items(breadcrumbs.size) { idx ->
                    val crumb = breadcrumbs[idx]
                    TextButton(onClick = { onNavigateBreadcrumb(idx) }) {
                        Text("${crumb.second} /")
                    }
                }
            }
        }

        if (items.isEmpty()) {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(16.dp)) {
                    Text(
                        if (!accountConnected) {
                            "Connect your Google Account in the 'Accounts' tab to browse Google Drive files."
                        } else {
                            "No files found in this view yet. Run a backup or tap Sync to refresh."
                        }
                    )
                }
            }
        } else if (isGridView) {
            LazyVerticalGrid(
                columns = GridCells.Fixed(2),
                verticalArrangement = Arrangement.spacedBy(8.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                modifier = Modifier.fillMaxSize()
            ) {
                items(items, key = { it.driveFileId }) { item ->
                    DriveFileCard(
                        item = item,
                        onOpenFolder = onOpenFolder,
                        onShowMetadata = { selectedMetadataItem = item },
                        onPreview = { onPreviewFile(item) },
                        onStreamExternal = { onStreamOpenExternal(item) },
                        onDownloadRestore = { onDownloadRestoreFile(item) },
                        onTogglePin = { onTogglePinOffline(item) },
                        onShareLink = { onShareFileLink(item) }
                    )
                }
            }
        } else {
            LazyColumn(
                verticalArrangement = Arrangement.spacedBy(8.dp),
                modifier = Modifier.fillMaxSize()
            ) {
                items(items, key = { it.driveFileId }) { item ->
                    DriveFileCard(
                        item = item,
                        onOpenFolder = onOpenFolder,
                        onShowMetadata = { selectedMetadataItem = item },
                        onPreview = { onPreviewFile(item) },
                        onStreamExternal = { onStreamOpenExternal(item) },
                        onDownloadRestore = { onDownloadRestoreFile(item) },
                        onTogglePin = { onTogglePinOffline(item) },
                        onShareLink = { onShareFileLink(item) }
                    )
                }
            }
        }
    }
}

@Composable
private fun DriveFileCard(
    item: CloudBrowseItem,
    onOpenFolder: (String, String) -> Unit,
    onShowMetadata: () -> Unit,
    onPreview: () -> Unit,
    onStreamExternal: () -> Unit,
    onDownloadRestore: () -> Unit,
    onTogglePin: () -> Unit,
    onShareLink: () -> Unit
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .clickable {
                if (item.isFolder) {
                    onOpenFolder(item.driveFileId, item.name)
                } else {
                    onShowMetadata()
                }
            }
    ) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(
                text = if (item.isFolder) "📁 ${item.name}" else "📄 ${item.name}",
                fontWeight = FontWeight.Bold,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
            if (!item.isFolder) {
                Text(
                    text = "${formatBytes(item.sizeBytes)} • ${item.mimeType} • Modified ${formatTimestamp(item.modifiedTimeMs)}",
                    style = MaterialTheme.typography.bodySmall
                )
                if (item.isProtectedBackupFile) {
                    Text(
                        text = "Protected Backup Resource (Never-Delete Guard Active)",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.primary
                    )
                }
                Spacer(modifier = Modifier.height(6.dp))
                LazyRow(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    item {
                        OutlinedButton(
                            onClick = onPreview,
                            contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Text("Preview", fontSize = 11.sp)
                        }
                    }
                    item {
                        OutlinedButton(
                            onClick = onStreamExternal,
                            contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Text("Stream / Open", fontSize = 11.sp)
                        }
                    }
                    item {
                        OutlinedButton(
                            onClick = onDownloadRestore,
                            contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Text("Download / Restore", fontSize = 11.sp)
                        }
                    }
                    item {
                        OutlinedButton(
                            onClick = onTogglePin,
                            contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Text(if (item.isPinnedOffline) "Unpin Offline" else "Keep Offline", fontSize = 11.sp)
                        }
                    }
                    item {
                        OutlinedButton(
                            onClick = onShareLink,
                            contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Text("Share Link", fontSize = 11.sp)
                        }
                    }
                    item {
                        OutlinedButton(
                            onClick = onShowMetadata,
                            contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Text("Info", fontSize = 11.sp)
                        }
                    }
                }
            } else {
                Text(
                    text = "Tap to open folder",
                    style = MaterialTheme.typography.bodySmall
                )
            }
        }
    }
}

/**
 * Section 5B & Section 12: Local Phone Explorer & Folder Coverage Management Screen.
 */
@Composable
fun PhoneStorageExplorerScreen(
    currentPath: String,
    localItems: List<LocalBrowseItem>,
    coverageItems: List<FolderCoverageItem>,
    onNavigateFolder: (String) -> Unit,
    onNavigateUp: () -> Unit,
    onGrantAllFilesAccess: () -> Unit,
    onSelectSafTree: () -> Unit
) {
    var showCoverageTab by remember { mutableStateOf(false) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Button(
                onClick = { showCoverageTab = false },
                modifier = Modifier.weight(1f),
                colors = ButtonDefaults.buttonColors(
                    containerColor = if (!showCoverageTab) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.secondaryContainer
                )
            ) {
                Text("Local File Browser")
            }
            Button(
                onClick = { showCoverageTab = true },
                modifier = Modifier.weight(1f),
                colors = ButtonDefaults.buttonColors(
                    containerColor = if (showCoverageTab) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.secondaryContainer
                )
            ) {
                Text("Folder Coverage (${coverageItems.size})")
            }
        }

        if (!showCoverageTab) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "Path: /${currentPath.ifBlank { "Internal Shared Storage" }}",
                    fontWeight = FontWeight.Bold,
                    style = MaterialTheme.typography.bodyMedium,
                    modifier = Modifier.weight(1f)
                )
                if (currentPath.isNotBlank()) {
                    OutlinedButton(onClick = onNavigateUp) {
                        Text("Up ..")
                    }
                }
            }

            if (localItems.isEmpty()) {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        Text("No accessible files in this directory or storage permission required.")
                        Button(onClick = onGrantAllFilesAccess) {
                            Text("Grant Storage Access")
                        }
                    }
                }
            } else {
                LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    items(localItems, key = { it.relativePath }) { item ->
                        Card(
                            modifier = Modifier
                                .fillMaxWidth()
                                .clickable(enabled = item.isDirectory && !item.isRestrictedByOs) {
                                    onNavigateFolder(item.relativePath)
                                }
                        ) {
                            Column(modifier = Modifier.padding(12.dp)) {
                                Text(
                                    text = if (item.isDirectory) "📁 ${item.name}" else "📄 ${item.name}",
                                    fontWeight = FontWeight.Bold
                                )
                                Text(
                                    text = if (item.isRestrictedByOs) {
                                        "Restricted by Android OS Sandbox (Never read or claimed)"
                                    } else if (item.isDirectory) {
                                        "Category: ${item.folderCategory} • ${item.childCount} items"
                                    } else {
                                        "${formatBytes(item.sizeBytes)} • ${item.mimeType} • Modified ${formatTimestamp(item.lastModifiedMs)}"
                                    },
                                    style = MaterialTheme.typography.bodySmall
                                )
                            }
                        }
                    }
                }
            }
        } else {
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = onGrantAllFilesAccess) {
                    Text("Grant All-Files Permission")
                }
                OutlinedButton(onClick = onSelectSafTree) {
                    Text("Add SAF Folder")
                }
            }
            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(coverageItems, key = { it.folderName + it.relativePath }) { item ->
                    Card(modifier = Modifier.fillMaxWidth()) {
                        Column(modifier = Modifier.padding(12.dp)) {
                            Text(item.folderName, fontWeight = FontWeight.Bold)
                            Text("Relative Path: ${item.relativePath}")
                            Text("Status: ${item.accessStatus} | Discovered Files: ${item.discoveredFileCount}")
                            Text("Completeness: ${(item.completenessFraction * 100).toInt()}% | Last Scan: ${formatTimestamp(item.lastScanTimeMs)}")
                            Text(item.restrictionExplanation, style = MaterialTheme.typography.bodySmall)
                        }
                    }
                }
            }
        }
    }
}

/**
 * Section 5F & 5G: Version History & Non-Overwriting File Restore Screen ("Backups").
 */
@Composable
fun BackupVersionHistoryScreen(
    verifiedHistory: List<BackupFileEntity>,
    actionFeedback: String?,
    onRestoreVersion: (BackupFileEntity) -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(
            text = "Permanent Cloud Backups & Version History (${verifiedHistory.size})",
            style = MaterialTheme.typography.titleMedium,
            fontWeight = FontWeight.Bold
        )
        Text(
            text = "Every file modification creates a new immutable version (v1, v2, ...). Restoring a version NEVER overwrites existing local files.",
            style = MaterialTheme.typography.bodySmall
        )
        actionFeedback?.let { msg ->
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.secondaryContainer),
                modifier = Modifier.fillMaxWidth()
            ) {
                Text(msg, modifier = Modifier.padding(10.dp), style = MaterialTheme.typography.bodySmall)
            }
        }
        LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            items(verifiedHistory, key = { it.id }) { item ->
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(12.dp)) {
                        Text(
                            text = "${item.driveStoredName} (Version ${item.backupVersion})",
                            fontWeight = FontWeight.Bold
                        )
                        Text("Original File: ${item.relativePath}")
                        Text("Drive File ID: ${item.driveFileId ?: "N/A"}")
                        Text("Size: ${formatBytes(item.fileSize)} | Integrity: ${item.verificationStatus}")
                        Text("SHA-256: ${item.sha256Hash?.take(24) ?: "Stream Verified"}...")
                        Text(
                            text = if (item.localExists) {
                                "Phone Status: Present on phone & backed up on Drive"
                            } else {
                                "Phone Status: Deleted on phone — PERMANENTLY PRESERVED ON GOOGLE DRIVE"
                            },
                            fontWeight = FontWeight.SemiBold,
                            color = if (item.localExists) MaterialTheme.colorScheme.onSurface else MaterialTheme.colorScheme.primary
                        )
                        Spacer(modifier = Modifier.height(6.dp))
                        OutlinedButton(onClick = { onRestoreVersion(item) }) {
                            Text("Restore Version ${item.backupVersion} (Non-Overwriting)")
                        }
                    }
                }
            }
        }
    }
}

/**
 * Section 12: Recent Files & Sync State Machine Event Log Screen ("Recent").
 */
@Composable
fun RecentActivityScreen(
    recentRecords: List<BackupFileEntity>,
    syncEvents: List<SyncEventEntity>,
    onRestoreRecord: (BackupFileEntity) -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            Text(
                text = "Recent Files & Sync State Machine Events",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
        }
        items(recentRecords.take(50), key = { "rec_${it.id}" }) { rec ->
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Text("${rec.driveStoredName} (v${rec.backupVersion})", fontWeight = FontWeight.Bold)
                    Text("Path: Android_Permanent_Backup/${rec.relativePath}")
                    Text("Status: ${rec.uploadStatus} • ${formatBytes(rec.fileSize)} • ${formatTimestamp(rec.createdAtMs)}")
                    if (rec.driveFileId != null) {
                        Spacer(modifier = Modifier.height(4.dp))
                        OutlinedButton(onClick = { onRestoreRecord(rec) }) {
                            Text("Restore to Phone", fontSize = 12.sp)
                        }
                    }
                }
            }
        }
        if (syncEvents.isNotEmpty()) {
            item {
                HorizontalDivider(modifier = Modifier.padding(vertical = 4.dp))
                Text(
                    text = "Sync Engine State Machine Audit Log (${syncEvents.size})",
                    style = MaterialTheme.typography.titleSmall,
                    fontWeight = FontWeight.Bold
                )
            }
            items(syncEvents.take(50), key = { "ev_${it.eventId}" }) { ev ->
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(10.dp)) {
                        Text(
                            text = "${ev.fromState ?: "START"} → ${ev.toState ?: ev.eventType}",
                            fontWeight = FontWeight.Bold,
                            fontSize = 12.sp
                        )
                        Text(ev.message, style = MaterialTheme.typography.bodySmall)
                        Text(formatTimestamp(ev.timestampMs), style = MaterialTheme.typography.bodySmall)
                    }
                }
            }
        }
    }
}

/**
 * Section 12: Transfer Manager Screen ("Transfers")
 * Shows File Name, File Size, Uploaded Bytes, Percentage, Transfer Speed, Estimated Remaining Time,
 * Pause, Resume, and Retry controls.
 */
@Composable
fun TransferManagerScreen(
    summary: DashboardSummary,
    settings: BackupSettings,
    activeQueue: List<BackupFileEntity>,
    failedList: List<BackupFileEntity>,
    onTriggerBackup: () -> Unit,
    onTogglePauseAll: () -> Unit,
    onPauseResumeSingle: (Long, Boolean) -> Unit,
    onRetryAll: () -> Unit,
    onRetrySingle: (Long) -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        item {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "Transfer Manager",
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.Bold
                )
                Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    OutlinedButton(onClick = onTogglePauseAll) {
                        Text(if (settings.backgroundSyncPaused) "Resume All" else "Pause All")
                    }
                    Button(onClick = onTriggerBackup) {
                        Text("Run Queue")
                    }
                }
            }
        }

        summary.currentlyUploading?.let { active ->
            item {
                Card(
                    colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.primaryContainer),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Column(modifier = Modifier.padding(14.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Text("Active Streaming Upload", fontWeight = FontWeight.Bold)
                        Text("File Name: ${active.fileName} (v${active.backupVersion})")
                        Text("File Size: ${formatBytes(active.totalBytes)}")
                        Text("Uploaded Bytes: ${formatBytes(active.uploadedBytes)} (${(active.fraction * 100).toInt()}%)")
                        Text("Transfer Speed: ${formatBytes(active.transferSpeedBps)}/s")
                        Text(
                            "Estimated Remaining Time: ${
                                active.estimatedRemainingSeconds?.let { "${it}s" } ?: "Calculating..."
                            }"
                        )
                        LinearProgressIndicator(
                            progress = { active.fraction },
                            modifier = Modifier.fillMaxWidth()
                        )
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            OutlinedButton(onClick = { onPauseResumeSingle(active.recordId, !active.isPaused) }) {
                                Text(if (active.isPaused) "Resume" else "Pause")
                            }
                        }
                    }
                }
            }
        }

        item {
            Text(
                text = "Queued & Resumable Transfers (${activeQueue.size})",
                fontWeight = FontWeight.Bold
            )
        }

        if (activeQueue.isEmpty()) {
            item {
                Text("No pending uploads in queue.", style = MaterialTheme.typography.bodySmall)
            }
        } else {
            items(activeQueue, key = { "q_${it.id}" }) { item ->
                val pct = if (item.fileSize > 0L) ((item.uploadedBytes * 100L) / item.fileSize).toInt() else 0
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(modifier = Modifier.padding(12.dp)) {
                        Text("${item.driveStoredName} (v${item.backupVersion})", fontWeight = FontWeight.Bold)
                        Text("Size: ${formatBytes(item.fileSize)} | Uploaded: ${formatBytes(item.uploadedBytes)} ($pct%)")
                        Text("State: ${item.uploadStatus} | Speed: ${formatBytes(summary.currentUploadSpeedBps)}/s")
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            OutlinedButton(onClick = { onPauseResumeSingle(item.id, true) }) {
                                Text("Pause", fontSize = 12.sp)
                            }
                            OutlinedButton(onClick = { onPauseResumeSingle(item.id, false) }) {
                                Text("Resume", fontSize = 12.sp)
                            }
                            OutlinedButton(onClick = { onRetrySingle(item.id) }) {
                                Text("Retry Now", fontSize = 12.sp)
                            }
                        }
                    }
                }
            }
        }

        item {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "Failed / Interrupted Transfers (${failedList.size})",
                    fontWeight = FontWeight.Bold
                )
                if (failedList.isNotEmpty()) {
                    OutlinedButton(onClick = onRetryAll) {
                        Text("Retry All")
                    }
                }
            }
        }

        items(failedList, key = { "f_${it.id}" }) { item ->
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Text("${item.driveStoredName} (v${item.backupVersion})", fontWeight = FontWeight.Bold)
                    Text("Reason: ${item.failureReason}")
                    item.failureDetail?.let { Text("Detail: $it", style = MaterialTheme.typography.bodySmall) }
                    Text("Saved Byte Offset: ${formatBytes(item.uploadedBytes)} / ${formatBytes(item.fileSize)}")
                    Spacer(modifier = Modifier.height(6.dp))
                    OutlinedButton(onClick = { onRetrySingle(item.id) }) {
                        Text("Resume / Retry Upload")
                    }
                }
            }
        }
    }
}

/**
 * Section 5E & Section 10: Offline Access & Low-Storage Cache Budget Screen ("Offline").
 */
@Composable
fun OfflineAccessScreen(
    pinnedFiles: List<CloudFileEntity>,
    usedBytes: Long,
    budgetBytes: Long,
    onUnpinFile: (CloudFileEntity) -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Text(
            text = "Offline Access (Cloud-First, Low-Storage Mode)",
            style = MaterialTheme.typography.titleMedium,
            fontWeight = FontWeight.Bold
        )
        Card(modifier = Modifier.fillMaxWidth()) {
            Column(modifier = Modifier.padding(14.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text("Offline Cache Budget Usage", fontWeight = FontWeight.Bold)
                Text("${formatBytes(usedBytes)} used of ${formatBytes(budgetBytes)} budget (${pinnedFiles.size} files pinned)")
                val frac = if (budgetBytes > 0L) (usedBytes.toFloat() / budgetBytes.toFloat()).coerceIn(0f, 1f) else 0f
                LinearProgressIndicator(progress = { frac }, modifier = Modifier.fillMaxWidth())
                Text(
                    "Default mode is Cloud-First with zero unnecessary cache. Unpinning a file only frees local phone cache and NEVER deletes the cloud file on Google Drive.",
                    style = MaterialTheme.typography.bodySmall
                )
            }
        }

        if (pinnedFiles.isEmpty()) {
            Text(
                "No files pinned for offline access yet. Tap 'Keep Offline' on any file in My Drive Explorer to pin it within your cache budget.",
                style = MaterialTheme.typography.bodySmall
            )
        } else {
            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(pinnedFiles, key = { it.driveFileId }) { file ->
                    Card(modifier = Modifier.fillMaxWidth()) {
                        Column(modifier = Modifier.padding(12.dp)) {
                            Text(file.name, fontWeight = FontWeight.Bold)
                            Text("Cached Size: ${formatBytes(file.offlineCachedBytes)} • ${file.mimeType}")
                            Text("Local Cache Path: ${file.offlineLocalPath ?: "N/A"}", style = MaterialTheme.typography.bodySmall)
                            Spacer(modifier = Modifier.height(6.dp))
                            OutlinedButton(onClick = { onUnpinFile(file) }) {
                                Text("Remove from Offline Cache (Keep on Drive)")
                            }
                        }
                    }
                }
            }
        }
    }
}

/**
 * Section 10 & 12: Settings Screen ("Settings").
 */
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
                text = "Synchronization, Storage Budget & Scheduling Settings",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold
            )
        }

        item {
            SettingToggleRow(
                title = "User Backup Consent (Required to Scan Local Files)",
                subtitle = "App will never read or upload any local file without explicit consent.",
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
            SettingToggleRow(
                title = "Pause Background Auto-Backup",
                subtitle = "Temporarily pause background synchronization while preserving queue state.",
                checked = settings.backgroundSyncPaused,
                onCheckedChange = { onUpdateSettings(settings.copy(backgroundSyncPaused = it)) }
            )
        }

        // Offline Cache Budget Selector (Section 10)
        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    Text(
                        "Offline File Cache Budget: ${formatBytes(settings.offlineCacheBudgetBytes)}",
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        "Strictly limits offline cache for low-storage phones (< 1 GB free space).",
                        style = MaterialTheme.typography.bodySmall
                    )
                    Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                        listOf(32L, 100L, 256L, 512L).forEach { mb ->
                            val bytes = mb * 1024L * 1024L
                            FilterChip(
                                selected = settings.offlineCacheBudgetBytes == bytes,
                                onClick = { onUpdateSettings(settings.copy(offlineCacheBudgetBytes = bytes)) },
                                label = { Text("$mb MB") }
                            )
                        }
                    }
                }
            }
        }

        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Text("Low-Storage Streaming Architecture (Section 10)", fontWeight = FontWeight.Bold)
                    Text("• Full-File Temporary Copy: FORBIDDEN (0 bytes temp copy)")
                    Text("• Full-File RAM Loading: FORBIDDEN (64 KiB fixed streaming buffer)")
                    Text("• Resumable Chunk Size: ${formatBytes(settings.chunkSizeBytes.toLong())}")
                    Text("• Periodic Scan Interval: Every ${settings.periodicIntervalHours} hours")
                    Text("• Remote Deletion Policy: PERMANENTLY DISABLED (3-Layer Guard)")
                }
            }
        }
    }
}

/**
 * Section 3, 4 & 5H: Multi-Account, Persistent Login & OAuth Configuration Diagnostics Screen ("Accounts").
 */
@Composable
fun GoogleAccountScreen(
    summary: DashboardSummary,
    savedAccounts: List<GoogleAccountEntity>,
    oauthDiagnostics: OAuthDiagnosticReport,
    signInFeedbackMessage: String? = null,
    onLaunchGoogleSignIn: () -> Unit,
    onGrantFullDriveReadScope: () -> Unit,
    onSwitchSavedAccount: (String) -> Unit,
    onVerifyCloudConnection: () -> Unit,
    onDisconnect: () -> Unit
) {
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
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text(
                        text = "Google Account & Persistent Session (গুগল ড্রাইভ একাউন্ট)",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        text = "Status: ${if (summary.accountState.isConnected) "Connected ✓ (${summary.accountState.authState})" else "Disconnected"}",
                        fontWeight = FontWeight.SemiBold,
                        color = if (summary.accountState.isConnected) {
                            MaterialTheme.colorScheme.primary
                        } else {
                            MaterialTheme.colorScheme.error
                        }
                    )
                    Text("Active Email: ${summary.accountState.email.ifBlank { "Not signed in" }}")
                    Text("Display Name: ${summary.accountState.displayName.ifBlank { "N/A" }}")
                    Text("Granted Scopes: ${summary.accountState.grantedScope}")
                    Text("Account Backup Root: ${summary.accountState.rootDriveFolderName} (${summary.accountState.rootDriveFolderId ?: "Auto-created on sync"})")
                    Text("Last Token Refresh: ${formatTimestamp(summary.accountState.lastTokenRefreshMs)} (${summary.accountState.lastTokenRefreshStatus})")
                    Text(
                        "Available Drive Storage: ${formatBytes(summary.driveQuota.availableBytes)} free of ${formatBytes(summary.driveQuota.limitBytes)}"
                    )
                    summary.accountState.connectionError?.let { err ->
                        Text("Notice: $err", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall)
                    }
                    signInFeedbackMessage?.let { msg ->
                        Text(msg, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.SemiBold)
                    }
                    Spacer(modifier = Modifier.height(6.dp))
                    Button(
                        onClick = onLaunchGoogleSignIn,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text(
                            if (summary.accountState.isConnected) {
                                "Add / Switch Google Account (নতুন একাউন্ট যুক্ত বা পরিবর্তন)"
                            } else {
                                "Connect Google Account (গুগল একাউন্ট সংযুক্ত করুন)"
                            }
                        )
                    }
                    if (summary.accountState.isConnected && !summary.accountState.hasFullDriveReadScope) {
                        OutlinedButton(
                            onClick = onGrantFullDriveReadScope,
                            modifier = Modifier.fillMaxWidth()
                        ) {
                            Text("Authorize Full My Drive Explorer Scope (drive.readonly)")
                        }
                    }
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        OutlinedButton(
                            onClick = onVerifyCloudConnection,
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Verify Drive Quota")
                        }
                        if (summary.accountState.isConnected) {
                            OutlinedButton(
                                onClick = onDisconnect,
                                modifier = Modifier.weight(1f)
                            ) {
                                Text("Sign Out")
                            }
                        }
                    }
                }
            }
        }

        // Section 5H: Multi-Account List
        if (savedAccounts.isNotEmpty()) {
            item {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(
                        modifier = Modifier.padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        Text(
                            "Saved Google Accounts (${savedAccounts.size}) — Isolated History & Roots",
                            fontWeight = FontWeight.Bold
                        )
                        savedAccounts.forEach { acc ->
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                horizontalArrangement = Arrangement.SpaceBetween,
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Column(modifier = Modifier.weight(1f)) {
                                    Text(
                                        text = "${acc.email} ${if (acc.isActive) "(Active)" else ""}",
                                        fontWeight = if (acc.isActive) FontWeight.Bold else FontWeight.Normal
                                    )
                                    Text(
                                        text = "Root ID: ${acc.backupRootFolderId ?: "Pending"} • State: ${acc.authorizationState}",
                                        style = MaterialTheme.typography.bodySmall
                                    )
                                }
                                if (!acc.isActive) {
                                    OutlinedButton(onClick = { onSwitchSavedAccount(acc.email) }) {
                                        Text("Switch", fontSize = 12.sp)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Section 4: OAuth & Signing Certificate Configuration Diagnostics
        item {
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(16.dp),
                    verticalArrangement = Arrangement.spacedBy(4.dp)
                ) {
                    Text(
                        text = "OAuth 2.0 & APK Signing Diagnostics (Section 4)",
                        fontWeight = FontWeight.Bold
                    )
                    Text("Android Package: ${oauthDiagnostics.packageName}", style = MaterialTheme.typography.bodySmall)
                    Text("Runtime Signing SHA-1: ${oauthDiagnostics.signingCertSha1}", style = MaterialTheme.typography.bodySmall)
                    Text("Runtime Signing SHA-256: ${oauthDiagnostics.signingCertSha256}", style = MaterialTheme.typography.bodySmall)
                    Text("Embedded Refresh Token in APK: NONE (Verified Zero Secrets)", style = MaterialTheme.typography.bodySmall)
                    Text("Legacy v1.3.0 Compromised Token Purged: ${oauthDiagnostics.legacyCompromisedTokensPurged}", style = MaterialTheme.typography.bodySmall)
                    Text(
                        text = "Google Cloud Console Requirement: Ensure an OAuth 2.0 Client ID of type 'Android' is registered for package 'com.permanentbackup.gdrive' with the above SHA-1 fingerprint and Google Drive API enabled.",
                        style = MaterialTheme.typography.bodySmall
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
