package com.permanentbackup.gdrive

import android.accounts.AccountManager
import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.IntentSenderRequest
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.provider.DriveStreamingContentProvider
import com.permanentbackup.gdrive.ui.screens.AppScreen
import com.permanentbackup.gdrive.ui.screens.BackupVersionHistoryScreen
import com.permanentbackup.gdrive.ui.screens.GoogleAccountScreen
import com.permanentbackup.gdrive.ui.screens.HomeDashboardScreen
import com.permanentbackup.gdrive.ui.screens.MyDriveExplorerScreen
import com.permanentbackup.gdrive.ui.screens.OfflineAccessScreen
import com.permanentbackup.gdrive.ui.screens.PhoneStorageExplorerScreen
import com.permanentbackup.gdrive.ui.screens.RecentActivityScreen
import com.permanentbackup.gdrive.ui.screens.SettingsScreen
import com.permanentbackup.gdrive.ui.screens.TransferManagerScreen
import com.permanentbackup.gdrive.ui.theme.PermanentBackupTheme
import com.permanentbackup.gdrive.ui.viewmodel.BackupViewModel

class MainActivity : ComponentActivity() {

    private val viewModel: BackupViewModel by viewModels()

    @OptIn(ExperimentalMaterial3Api::class)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            PermanentBackupTheme {
                var currentScreen by remember { mutableStateOf(AppScreen.HOME) }
                var pendingSelectedEmail by remember { mutableStateOf("") }
                var pendingFullReadScope by remember { mutableStateOf(false) }

                val summary by viewModel.dashboardSummary.collectAsState()
                val activeQueue by viewModel.activeQueue.collectAsState()
                val verifiedHistory by viewModel.verifiedHistory.collectAsState()
                val recentRecords by viewModel.recentRecords.collectAsState()
                val recentSyncEvents by viewModel.recentSyncEvents.collectAsState()
                val failedUploads by viewModel.failedUploads.collectAsState()
                val folderCoverage by viewModel.folderCoverage.collectAsState()
                val cloudDriveItems by viewModel.cloudDriveItems.collectAsState()
                val driveFilter by viewModel.driveFilter.collectAsState()
                val driveSortOrder by viewModel.driveSortOrder.collectAsState()
                val driveBreadcrumbs by viewModel.driveFolderBreadcrumbs.collectAsState()
                val driveSearchQuery by viewModel.driveSearchQuery.collectAsState()
                val isDriveGridView by viewModel.isDriveGridView.collectAsState()
                val localPhoneItems by viewModel.localPhoneItems.collectAsState()
                val currentLocalPath by viewModel.currentLocalPath.collectAsState()
                val pinnedOfflineFiles by viewModel.pinnedOfflineFiles.collectAsState()
                val savedAccounts by viewModel.savedAccounts.collectAsState()
                val oauthDiagnostics by viewModel.oauthDiagnostics.collectAsState()
                val settings by viewModel.settings.collectAsState()
                val signInFeedbackMessage by viewModel.signInStatusMessage.collectAsState()
                val actionFeedbackMessage by viewModel.actionFeedbackMessage.collectAsState()
                val previewSnippet by viewModel.previewSnippet.collectAsState()

                val safTreeLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.OpenDocumentTree()
                ) { treeUri: Uri? ->
                    if (treeUri != null) {
                        contentResolver.takePersistableUriPermission(
                            treeUri,
                            Intent.FLAG_GRANT_READ_URI_PERMISSION
                        )
                        viewModel.refreshDashboardAndCoverage()
                    }
                }

                // Official Google Identity Services AuthorizationClient PendingIntent resolution launcher
                val identityAuthorizationLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.StartIntentSenderForResult()
                ) {
                    if (pendingSelectedEmail.isNotBlank()) {
                        viewModel.completeNativeConsentResult(
                            context = this@MainActivity,
                            selectedEmail = pendingSelectedEmail,
                            requestFullDriveRead = pendingFullReadScope
                        )
                    }
                }

                // Fallback Play Services Consent Intent Launcher
                val recoverableConsentLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.StartActivityForResult()
                ) {
                    if (pendingSelectedEmail.isNotBlank()) {
                        viewModel.completeNativeConsentResult(
                            context = this@MainActivity,
                            selectedEmail = pendingSelectedEmail,
                            requestFullDriveRead = pendingFullReadScope
                        )
                    }
                }

                // Native Android System Account Picker -> Followed by Official Drive AuthorizationClient
                val nativeAccountPickerLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.StartActivityForResult()
                ) { result ->
                    if (result.resultCode == Activity.RESULT_OK) {
                        val selectedEmail = result.data
                            ?.getStringExtra(AccountManager.KEY_ACCOUNT_NAME)
                            .orEmpty()
                        if (selectedEmail.isNotBlank()) {
                            pendingSelectedEmail = selectedEmail
                            viewModel.onNativeGoogleAccountSelected(
                                context = this@MainActivity,
                                selectedEmail = selectedEmail,
                                requestFullDriveRead = pendingFullReadScope,
                                onResolutionPendingIntent = { pendingIntent ->
                                    identityAuthorizationLauncher.launch(
                                        IntentSenderRequest.Builder(pendingIntent).build()
                                    )
                                },
                                onRecoverableConsentIntent = { consentIntent ->
                                    recoverableConsentLauncher.launch(consentIntent)
                                }
                            )
                        }
                    }
                }

                val launchNativeGoogleAccountPicker: (Boolean) -> Unit = { requestFullRead ->
                    pendingFullReadScope = requestFullRead
                    try {
                        @Suppress("DEPRECATION")
                        val pickerIntent = AccountManager.newChooseAccountIntent(
                            null,
                            null,
                            arrayOf("com.google"),
                            null,
                            null,
                            null,
                            null
                        )
                        nativeAccountPickerLauncher.launch(pickerIntent)
                    } catch (_: Throwable) {
                        viewModel.verifyAndRefreshCloudConnection()
                    }
                }

                val authorizeAdditionalDriveReadScope: () -> Unit = {
                    val activeEmail = summary.accountState.email
                    if (activeEmail.isNotBlank()) {
                        pendingSelectedEmail = activeEmail
                        pendingFullReadScope = true
                        viewModel.onNativeGoogleAccountSelected(
                            context = this@MainActivity,
                            selectedEmail = activeEmail,
                            requestFullDriveRead = true,
                            onResolutionPendingIntent = { pendingIntent ->
                                identityAuthorizationLauncher.launch(
                                    IntentSenderRequest.Builder(pendingIntent).build()
                                )
                            },
                            onRecoverableConsentIntent = { consentIntent ->
                                recoverableConsentLauncher.launch(consentIntent)
                            }
                        )
                    } else {
                        launchNativeGoogleAccountPicker(true)
                    }
                }

                Scaffold(
                    modifier = Modifier
                        .fillMaxSize()
                        .statusBarsPadding()
                        .navigationBarsPadding(),
                    topBar = {
                        Column {
                            TopAppBar(
                                title = {
                                    Text(
                                        text = "Permanent Drive — ${currentScreen.title}",
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                }
                            )
                            TwoRowNavigationHeader(
                                currentScreen = currentScreen,
                                onSelectScreen = { selected ->
                                    currentScreen = selected
                                    viewModel.refreshDashboardAndCoverage()
                                }
                            )
                            HorizontalDivider()
                        }
                    }
                ) { innerPadding ->
                    Box(
                        modifier = Modifier
                            .fillMaxSize()
                            .padding(innerPadding)
                    ) {
                        when (currentScreen) {
                            AppScreen.HOME -> HomeDashboardScreen(
                                summary = summary,
                                settings = settings,
                                actionFeedback = actionFeedbackMessage,
                                onBackupNow = { viewModel.triggerManualBackupNow() },
                                onConnectOrManageAccount = {
                                    currentScreen = AppScreen.ACCOUNTS
                                    // Section 3: Never prompt Account Picker if already connected!
                                    if (!summary.accountState.isConnected) {
                                        launchNativeGoogleAccountPicker(false)
                                    }
                                },
                                onOpenPhoneStorageScreen = {
                                    currentScreen = AppScreen.PHONE_STORAGE
                                    viewModel.refreshLocalPhoneBrowser("")
                                },
                                onOpenMyDriveScreen = {
                                    currentScreen = AppScreen.MY_DRIVE
                                    viewModel.refreshCloudDriveExplorer()
                                },
                                onEnableConsentAndPermissions = {
                                    viewModel.updateSettings(settings.copy(userBackupConsentGranted = true))
                                    requestManageAllFilesAccess()
                                },
                                onRefresh = { viewModel.refreshDashboardAndCoverage(fetchRemoteQuota = true) }
                            )
                            AppScreen.MY_DRIVE -> MyDriveExplorerScreen(
                                accountConnected = summary.accountState.isConnected,
                                hasFullDriveReadScope = summary.accountState.hasFullDriveReadScope,
                                items = cloudDriveItems,
                                currentFilter = driveFilter,
                                currentSort = driveSortOrder,
                                breadcrumbs = driveBreadcrumbs,
                                searchQuery = driveSearchQuery,
                                isGridView = isDriveGridView,
                                previewSnippet = previewSnippet,
                                onSelectFilter = { viewModel.setDriveFilter(it) },
                                onSelectSort = { viewModel.setDriveSortOrder(it) },
                                onSearchQueryChange = { viewModel.setDriveSearchQuery(it) },
                                onToggleGridView = { viewModel.toggleDriveGridView() },
                                onOpenFolder = { id, name -> viewModel.openDriveSubfolder(id, name) },
                                onNavigateBreadcrumb = { idx -> viewModel.navigateUpDriveFolder(idx) },
                                onPreviewFile = { viewModel.previewCloudFileStream(it) },
                                onDismissPreview = { viewModel.dismissPreviewDialog() },
                                onStreamOpenExternal = { openCloudStreamWithSupportedApp(it) },
                                onDownloadRestoreFile = { viewModel.restoreCloudFile(it) },
                                onTogglePinOffline = { viewModel.toggleCloudFileOfflinePin(it) },
                                onShareFileLink = { shareCloudFileLink(it) },
                                onRequestFullDriveReadScope = authorizeAdditionalDriveReadScope,
                                onRefresh = { viewModel.refreshCloudDriveExplorer() }
                            )
                            AppScreen.PHONE_STORAGE -> PhoneStorageExplorerScreen(
                                currentPath = currentLocalPath,
                                localItems = localPhoneItems,
                                coverageItems = folderCoverage,
                                onNavigateFolder = { viewModel.refreshLocalPhoneBrowser(it) },
                                onNavigateUp = { viewModel.navigateUpLocalFolder() },
                                onGrantAllFilesAccess = {
                                    viewModel.updateSettings(settings.copy(userBackupConsentGranted = true))
                                    requestManageAllFilesAccess()
                                },
                                onSelectSafTree = { safTreeLauncher.launch(null) },
                                onToggleFolderBackup = { item, enabled ->
                                    viewModel.toggleFolderBackup(item, enabled)
                                },
                                onBackupSingleFile = { item ->
                                    viewModel.backupSingleLocalFile(item)
                                }
                            )
                            AppScreen.BACKUPS -> BackupVersionHistoryScreen(
                                verifiedHistory = verifiedHistory,
                                actionFeedback = actionFeedbackMessage,
                                onRestoreVersion = { viewModel.restoreBackupVersion(it) }
                            )
                            AppScreen.RECENT -> RecentActivityScreen(
                                recentRecords = recentRecords,
                                syncEvents = recentSyncEvents,
                                onRestoreRecord = { viewModel.restoreBackupVersion(it) }
                            )
                            AppScreen.TRANSFERS -> TransferManagerScreen(
                                summary = summary,
                                settings = settings,
                                activeQueue = activeQueue,
                                failedList = failedUploads,
                                onTriggerBackup = { viewModel.triggerManualBackupNow() },
                                onTogglePauseAll = { viewModel.togglePauseAllTransfers() },
                                onPauseResumeSingle = { id, pause -> viewModel.togglePauseSingleRecord(id, pause) },
                                onRetryAll = { viewModel.retryFailedUploads() },
                                onRetrySingle = { id -> viewModel.retrySingleRecord(id) }
                            )
                            AppScreen.OFFLINE -> OfflineAccessScreen(
                                pinnedFiles = pinnedOfflineFiles,
                                usedBytes = summary.offlineCacheUsedBytes,
                                budgetBytes = summary.offlineCacheBudgetBytes,
                                onUnpinFile = { viewModel.unpinOfflineEntity(it) }
                            )
                            AppScreen.SETTINGS -> SettingsScreen(
                                settings = settings,
                                onUpdateSettings = { viewModel.updateSettings(it) }
                            )
                            AppScreen.ACCOUNTS -> GoogleAccountScreen(
                                summary = summary,
                                savedAccounts = savedAccounts,
                                oauthDiagnostics = oauthDiagnostics,
                                signInFeedbackMessage = signInFeedbackMessage,
                                onLaunchGoogleSignIn = { launchNativeGoogleAccountPicker(false) },
                                onGrantFullDriveReadScope = authorizeAdditionalDriveReadScope,
                                onSwitchSavedAccount = { email ->
                                    pendingSelectedEmail = email
                                    viewModel.switchActiveSavedAccount(this@MainActivity, email)
                                },
                                onVerifyCloudConnection = { viewModel.verifyAndRefreshCloudConnection() },
                                onDisconnect = { viewModel.disconnectGoogleAccount() }
                            )
                        }
                    }
                }
            }
        }
    }

    private fun openCloudStreamWithSupportedApp(item: CloudBrowseItem) {
        try {
            val streamUri = DriveStreamingContentProvider.buildStreamUri(
                driveFileId = item.driveFileId,
                displayName = item.name,
                mimeType = item.mimeType,
                sizeBytes = item.sizeBytes
            )
            val viewIntent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(streamUri, item.mimeType)
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(Intent.createChooser(viewIntent, "Open Stream: ${item.name}"))
        } catch (_: Throwable) {
        }
    }

    private fun shareCloudFileLink(item: CloudBrowseItem) {
        try {
            val shareLink = item.webViewLink
                ?: "https://drive.google.com/file/d/${item.driveFileId}/view"
            val sendIntent = Intent(Intent.ACTION_SEND).apply {
                type = "text/plain"
                putExtra(Intent.EXTRA_SUBJECT, item.name)
                putExtra(Intent.EXTRA_TEXT, "${item.name}: $shareLink")
            }
            startActivity(Intent.createChooser(sendIntent, "Share Google Drive Link"))
        } catch (_: Throwable) {
        }
    }

    private fun requestManageAllFilesAccess() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            try {
                val intent = Intent(
                    Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION,
                    Uri.parse("package:$packageName")
                )
                startActivity(intent)
            } catch (_: Throwable) {
                startActivity(Intent(Settings.ACTION_MANAGE_ALL_FILES_ACCESS_PERMISSION))
            }
        }
    }
}

@Composable
private fun TwoRowNavigationHeader(
    currentScreen: AppScreen,
    onSelectScreen: (AppScreen) -> Unit
) {
    val row1 = listOf(
        AppScreen.HOME,
        AppScreen.MY_DRIVE,
        AppScreen.PHONE_STORAGE,
        AppScreen.BACKUPS,
        AppScreen.RECENT
    )
    val row2 = listOf(
        AppScreen.TRANSFERS,
        AppScreen.OFFLINE,
        AppScreen.SETTINGS,
        AppScreen.ACCOUNTS
    )

    Surface(
        color = MaterialTheme.colorScheme.surface,
        modifier = Modifier.fillMaxWidth()
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 6.dp, vertical = 6.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                row1.forEach { screen ->
                    NavChipButton(
                        screen = screen,
                        selected = currentScreen == screen,
                        modifier = Modifier.weight(1f),
                        onClick = { onSelectScreen(screen) }
                    )
                }
            }
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                row2.forEach { screen ->
                    NavChipButton(
                        screen = screen,
                        selected = currentScreen == screen,
                        modifier = Modifier.weight(1f),
                        onClick = { onSelectScreen(screen) }
                    )
                }
            }
        }
    }
}

@Composable
private fun NavChipButton(
    screen: AppScreen,
    selected: Boolean,
    modifier: Modifier = Modifier,
    onClick: () -> Unit
) {
    if (selected) {
        Button(
            onClick = onClick,
            modifier = modifier,
            contentPadding = PaddingValues(horizontal = 2.dp, vertical = 6.dp),
            colors = ButtonDefaults.buttonColors(
                containerColor = MaterialTheme.colorScheme.primary
            )
        ) {
            Text(
                text = screen.title,
                fontSize = 11.sp,
                fontWeight = FontWeight.Bold,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
    } else {
        OutlinedButton(
            onClick = onClick,
            modifier = modifier,
            contentPadding = PaddingValues(horizontal = 2.dp, vertical = 6.dp)
        ) {
            Text(
                text = screen.title,
                fontSize = 11.sp,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
    }
}
