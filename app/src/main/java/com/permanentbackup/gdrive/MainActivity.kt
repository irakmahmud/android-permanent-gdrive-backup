package com.permanentbackup.gdrive

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
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.ScrollableTabRow
import androidx.compose.material3.Tab
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import com.google.android.gms.auth.api.signin.GoogleSignIn
import com.google.android.gms.auth.api.signin.GoogleSignInOptions
import com.google.android.gms.common.api.Scope
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
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

                val summary by viewModel.dashboardSummary.collectAsState()
                val activeQueue by viewModel.activeQueue.collectAsState()
                val verifiedHistory by viewModel.verifiedHistory.collectAsState()
                val failedUploads by viewModel.failedUploads.collectAsState()
                val folderCoverage by viewModel.folderCoverage.collectAsState()
                val settings by viewModel.settings.collectAsState()

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

                val googleSignInLauncher = rememberLauncherForActivityResult(
                    contract = ActivityResultContracts.StartActivityForResult()
                ) { result ->
                    val task = GoogleSignIn.getSignedInAccountFromIntent(result.data)
                    try {
                        val account = task.result
                        if (account != null) {
                            viewModel.connectGoogleAccount(
                                email = account.email ?: "connected@gmail.com",
                                displayName = account.displayName ?: "Google User",
                                accessToken = account.idToken ?: account.serverAuthCode ?: "oauth_session_active",
                                refreshToken = account.serverAuthCode
                            )
                        }
                    } catch (_: Throwable) {
                    }
                }

                Scaffold(
                    topBar = {
                        TopAppBar(
                            title = { Text("Permanent Drive Backup — ${currentScreen.title}") }
                        )
                    },
                    bottomBar = {
                        ScrollableTabRow(selectedTabIndex = currentScreen.ordinal) {
                            AppScreen.entries.forEach { screen ->
                                Tab(
                                    selected = currentScreen == screen,
                                    onClick = {
                                        currentScreen = screen
                                        viewModel.refreshDashboardAndCoverage()
                                    },
                                    text = { Text(screen.title) }
                                )
                            }
                        }
                    }
                ) { innerPadding ->
                    Box(modifier = Modifier.padding(innerPadding)) {
                        when (currentScreen) {
                            AppScreen.DASHBOARD -> DashboardScreen(
                                summary = summary,
                                onBackupNow = { viewModel.triggerManualBackupNow() },
                                onOpenPermissions = { requestManageAllFilesAccess() },
                                onRefresh = { viewModel.refreshDashboardAndCoverage() }
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
                            AppScreen.FOLDERS -> FolderCoverageScreen(
                                coverageItems = folderCoverage,
                                onGrantAllFilesAccess = { requestManageAllFilesAccess() },
                                onSelectSafTree = { safTreeLauncher.launch(null) }
                            )
                            AppScreen.SETTINGS -> SettingsScreen(
                                settings = settings,
                                onUpdateSettings = { viewModel.updateSettings(it) }
                            )
                            AppScreen.ACCOUNT -> GoogleAccountScreen(
                                summary = summary,
                                onLaunchGoogleSignIn = {
                                    val gsoBuilder = GoogleSignInOptions.Builder(GoogleSignInOptions.DEFAULT_SIGN_IN)
                                        .requestEmail()
                                        .requestScopes(Scope(GoogleAuthManager.LEAST_PRIVILEGE_SCOPE))
                                    if (BuildConfig.GOOGLE_OAUTH_CLIENT_ID.isNotBlank()) {
                                        gsoBuilder.requestServerAuthCode(BuildConfig.GOOGLE_OAUTH_CLIENT_ID, true)
                                    }
                                    val client = GoogleSignIn.getClient(this@MainActivity, gsoBuilder.build())
                                    googleSignInLauncher.launch(client.signInIntent)
                                },
                                onConnectTokenManually = { email, name, token, refresh ->
                                    viewModel.connectGoogleAccount(email, name, token, refresh)
                                },
                                onSwitchAccount = { email, name, token, refresh ->
                                    viewModel.switchGoogleAccount(email, name, token, refresh)
                                },
                                onDisconnect = { viewModel.disconnectGoogleAccount() }
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
