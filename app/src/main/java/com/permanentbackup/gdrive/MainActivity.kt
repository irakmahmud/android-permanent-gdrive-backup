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
import com.permanentbackup.gdrive.ui.screens.AppScreen
import com.permanentbackup.gdrive.ui.screens.BackupActivityScreen
import com.permanentbackup.gdrive.ui.screens.BackupHistoryScreen
import com.permanentbackup.gdrive.ui.screens.DashboardScreen
import com.permanentbackup.gdrive.ui.screens.FailedUploadsScreen
import com.permanentbackup.gdrive.ui.screens.FolderCoverageScreen
import com.permanentbackup.gdrive.ui.screens.GoogleAccountScreen
import com.permanentbackup.gdrive.ui.screens.SettingsScreen
import com.permanentbackup.gdrive.ui.theme.PermanentBackupTheme
import com.permanentbackup.gdrive.ui.viewmodel.BackupViewModel

class MainActivity : ComponentActivity() {

    private val viewModel: BackupViewModel by viewModels()

    @OptIn(ExperimentalMaterial3Api::class)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            PermanentBackupTheme {
                var currentScreen by remember { mutableStateOf(AppScreen.DASHBOARD) }
                var pendingSelectedEmail by remember { mutableStateOf("") }

                val summary by viewModel.dashboardSummary.collectAsState()
                val activeQueue by viewModel.activeQueue.collectAsState()
                val verifiedHistory by viewModel.verifiedHistory.collectAsState()
                val failedUploads by viewModel.failedUploads.collectAsState()
                val folderCoverage by viewModel.folderCoverage.collectAsState()
                val settings by viewModel.settings.collectAsState()
                val signInFeedbackMessage by viewModel.signInStatusMessage.collectAsState()

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

                // Native Play Services Consent Launcher (if Play Services requests Drive permission sheet)
                val recoverableConsentLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.StartActivityForResult()
                ) {
                    if (pendingSelectedEmail.isNotBlank()) {
                        viewModel.completeNativeConsentResult(this@MainActivity, pendingSelectedEmail)
                    }
                }

                // 100% Native Android System Google Account Picker (Zero Browser, Zero Localhost, Zero Code-10!)
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
                                onRecoverableConsentIntent = { consentIntent ->
                                    recoverableConsentLauncher.launch(consentIntent)
                                }
                            )
                        }
                    }
                }

                val launchNativeGoogleAccountPicker: () -> Unit = {
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
                                        text = "Permanent Backup — ${currentScreen.title}",
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                }
                            )
                            // Always-visible 2-row Navigation Grid at top so NO button is ever hidden or cut off!
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
                            AppScreen.DASHBOARD -> DashboardScreen(
                                summary = summary,
                                settings = settings,
                                onBackupNow = { viewModel.triggerManualBackupNow() },
                                onOpenAccountScreen = {
                                    currentScreen = AppScreen.ACCOUNT
                                    launchNativeGoogleAccountPicker()
                                },
                                onOpenFoldersScreen = { currentScreen = AppScreen.FOLDERS },
                                onEnableConsentAndPermissions = {
                                    viewModel.updateSettings(settings.copy(userBackupConsentGranted = true))
                                    requestManageAllFilesAccess()
                                },
                                onRefresh = { viewModel.refreshDashboardAndCoverage(fetchRemoteQuota = true) }
                            )
                            AppScreen.ACCOUNT -> GoogleAccountScreen(
                                summary = summary,
                                signInFeedbackMessage = signInFeedbackMessage,
                                onLaunchGoogleSignIn = launchNativeGoogleAccountPicker,
                                onLaunchBrowserOAuth = { viewModel.verifyAndRefreshCloudConnection() },
                                onConnectTokenManually = { email, name, token, refresh ->
                                    viewModel.connectGoogleAccount(email, name, token, refresh)
                                },
                                onSwitchAccount = { email, name, token, refresh ->
                                    viewModel.switchGoogleAccount(email, name, token, refresh)
                                },
                                onDisconnect = {
                                    viewModel.disconnectGoogleAccount()
                                }
                            )
                            AppScreen.FOLDERS -> FolderCoverageScreen(
                                coverageItems = folderCoverage,
                                onGrantAllFilesAccess = {
                                    viewModel.updateSettings(settings.copy(userBackupConsentGranted = true))
                                    requestManageAllFilesAccess()
                                },
                                onSelectSafTree = { safTreeLauncher.launch(null) }
                            )
                            AppScreen.SETTINGS -> SettingsScreen(
                                settings = settings,
                                onUpdateSettings = { viewModel.updateSettings(it) }
                            )
                            AppScreen.ACTIVITY -> BackupActivityScreen(
                                activeQueue = activeQueue,
                                onTriggerBackup = { viewModel.triggerManualBackupNow() }
                            )
                            AppScreen.HISTORY -> BackupHistoryScreen(
                                verifiedHistory = verifiedHistory
                            )
                            AppScreen.FAILED -> FailedUploadsScreen(
                                failedList = failedUploads,
                                onRetryAll = { viewModel.retryFailedUploads() },
                                onRetrySingle = { id -> viewModel.retrySingleRecord(id) }
                            )
                        }
                    }
                }
            }
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
    val row1 = listOf(AppScreen.DASHBOARD, AppScreen.ACCOUNT, AppScreen.FOLDERS, AppScreen.SETTINGS)
    val row2 = listOf(AppScreen.ACTIVITY, AppScreen.HISTORY, AppScreen.FAILED)

    Surface(
        color = MaterialTheme.colorScheme.surface,
        modifier = Modifier.fillMaxWidth()
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 8.dp, vertical = 6.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(6.dp)
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
                horizontalArrangement = Arrangement.spacedBy(6.dp)
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
            contentPadding = PaddingValues(horizontal = 4.dp, vertical = 6.dp),
            colors = ButtonDefaults.buttonColors(
                containerColor = MaterialTheme.colorScheme.primary
            )
        ) {
            Text(
                text = screen.title,
                fontSize = 12.sp,
                fontWeight = FontWeight.Bold,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
    } else {
        OutlinedButton(
            onClick = onClick,
            modifier = modifier,
            contentPadding = PaddingValues(horizontal = 4.dp, vertical = 6.dp)
        ) {
            Text(
                text = screen.title,
                fontSize = 12.sp,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
    }
}
