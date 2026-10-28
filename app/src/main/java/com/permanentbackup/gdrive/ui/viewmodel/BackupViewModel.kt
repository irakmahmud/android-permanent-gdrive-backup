package com.permanentbackup.gdrive.ui.viewmodel

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.service.PermanentBackupForegroundService
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class BackupViewModel(application: Application) : AndroidViewModel(application) {
    private val app = application as PermanentBackupApp
    private val repository = app.repository

    private val _dashboardSummary = MutableStateFlow(DashboardSummary())
    val dashboardSummary: StateFlow<DashboardSummary> = _dashboardSummary.asStateFlow()

    private val _folderCoverage = MutableStateFlow<List<FolderCoverageItem>>(emptyList())
    val folderCoverage: StateFlow<List<FolderCoverageItem>> = _folderCoverage.asStateFlow()

    private val _signInStatusMessage = MutableStateFlow<String?>(null)
    val signInStatusMessage: StateFlow<String?> = _signInStatusMessage.asStateFlow()

    val settings: StateFlow<BackupSettings> = repository.settings

    val activeQueue: StateFlow<List<BackupFileEntity>> = repository.observeActiveQueue()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val verifiedHistory: StateFlow<List<BackupFileEntity>> = repository.observeVerifiedHistory()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val failedUploads: StateFlow<List<BackupFileEntity>> = repository.observeFailedUploads()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    init {
        refreshDashboardAndCoverage(fetchRemoteQuota = true)
    }

    fun refreshDashboardAndCoverage(fetchRemoteQuota: Boolean = false) {
        viewModelScope.launch {
            if (fetchRemoteQuota && repository.authManager.accountState.value.isConnected) {
                withContext(Dispatchers.IO) {
                    repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                }
            }
            _dashboardSummary.value = repository.buildDashboardSummary()
            _folderCoverage.value = repository.getFolderCoverageItems()
        }
    }

    fun triggerManualBackupNow() {
        PermanentBackupForegroundService.startManualBackupService(getApplication())
        viewModelScope.launch(Dispatchers.IO) {
            repository.runBackupNow()
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun retryFailedUploads() {
        viewModelScope.launch(Dispatchers.IO) {
            repository.retryAllFailedUploads()
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun retrySingleRecord(recordId: Long) {
        viewModelScope.launch(Dispatchers.IO) {
            repository.requeueFailedRecord(recordId)
            repository.runBackupNow()
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun updateSettings(newSettings: BackupSettings) {
        repository.updateSettings(newSettings)
        BackupWorkScheduler.schedulePeriodicBackup(getApplication(), newSettings)
        refreshDashboardAndCoverage()
    }

    /**
     * 1-Tap Google Sign-In using OAuth 2.0 Refresh Token & Drive v3 Verification (Zero Code-10 errors!).
     */
    fun signInOneTapCloudAccount(onFallbackBrowserNeeded: (String) -> Unit) {
        viewModelScope.launch {
            _signInStatusMessage.value = "Connecting to Google Drive via OAuth 2.0..."
            val connected = withContext(Dispatchers.IO) {
                try {
                    val ok = repository.authManager.connectUsingConfiguredCloudRefreshToken()
                    if (ok) {
                        repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    }
                    ok
                } catch (_: Throwable) {
                    false
                }
            }
            if (connected) {
                val email = repository.authManager.accountState.value.email
                _signInStatusMessage.value = "✓ Google Drive Connected ($email) & 'Android_Permanent_Backup' Ready!"
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            } else {
                val url = repository.authManager.buildBrowserOAuthUrl()
                onFallbackBrowserNeeded(url)
                listenForBrowserOAuthCallback()
            }
        }
    }

    fun startBrowserOAuthSignIn(onOpenUrl: (String) -> Unit) {
        val url = repository.authManager.buildBrowserOAuthUrl()
        _signInStatusMessage.value = "Browser opened — please select your Google Account and tap Allow..."
        onOpenUrl(url)
        listenForBrowserOAuthCallback()
    }

    private fun listenForBrowserOAuthCallback() {
        viewModelScope.launch {
            try {
                val email = withContext(Dispatchers.IO) {
                    val authorizedEmail = repository.authManager.awaitLoopbackOAuthCallbackAndExchange(
                        repository.driveApiClient
                    )
                    repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    authorizedEmail
                }
                _signInStatusMessage.value = "✓ Connected via Browser OAuth ($email)!"
                refreshDashboardAndCoverage()
            } catch (e: Exception) {
                _signInStatusMessage.value = "Browser Sign-In status: ${e.message}"
            }
        }
    }

    fun connectGoogleAccount(email: String, displayName: String, accessToken: String, refreshToken: String?) {
        viewModelScope.launch {
            withContext(Dispatchers.IO) {
                repository.authManager.onAccountAuthorized(
                    email = email,
                    displayName = displayName,
                    accessToken = accessToken,
                    refreshToken = refreshToken
                )
                repository.backupEngine.refreshRemoteQuotaAndRootFolder()
            }
            _signInStatusMessage.value = "✓ Google Account ($email) connected!"
            refreshDashboardAndCoverage()
        }
    }

    fun switchGoogleAccount(newEmail: String, newDisplayName: String, newAccessToken: String, newRefreshToken: String?) {
        viewModelScope.launch {
            withContext(Dispatchers.IO) {
                repository.authManager.switchGoogleAccount(
                    newEmail = newEmail,
                    newDisplayName = newDisplayName,
                    newAccessToken = newAccessToken,
                    newRefreshToken = newRefreshToken
                )
                repository.backupEngine.refreshRemoteQuotaAndRootFolder()
            }
            _signInStatusMessage.value = "✓ Switched to Google Account ($newEmail)!"
            refreshDashboardAndCoverage()
        }
    }

    fun disconnectGoogleAccount() {
        repository.authManager.disconnectAccount()
        _signInStatusMessage.value = "Google Account disconnected."
        refreshDashboardAndCoverage()
    }
}
