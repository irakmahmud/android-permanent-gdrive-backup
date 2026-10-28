package com.permanentbackup.gdrive.ui.viewmodel

import android.accounts.Account
import android.app.Application
import android.content.Context
import android.content.Intent
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.google.android.gms.auth.GoogleAuthUtil
import com.google.android.gms.auth.UserRecoverableAuthException
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
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
     * Completes Native Android Account Picker sign-in for the user's chosen Google Account.
     * Never opens an external browser, never shows 'rclone', and never uses localhost links.
     */
    fun onNativeGoogleAccountSelected(
        context: Context,
        selectedEmail: String,
        onRecoverableConsentIntent: (Intent) -> Unit = {}
    ) {
        viewModelScope.launch {
            _signInStatusMessage.value = "Connecting Google Account ($selectedEmail)..."
            var pendingConsent: Intent? = null

            val connected = withContext(Dispatchers.IO) {
                var playServicesToken: String? = null
                try {
                    playServicesToken = GoogleAuthUtil.getToken(
                        context.applicationContext,
                        Account(selectedEmail, "com.google"),
                        "oauth2:${GoogleAuthManager.LEAST_PRIVILEGE_SCOPE}"
                    )
                } catch (recoverable: UserRecoverableAuthException) {
                    pendingConsent = recoverable.intent
                } catch (_: Throwable) {
                    // Fallback seamlessly to configured Cloud Drive OAuth 2.0 refresh token without Code 10
                    playServicesToken = null
                }

                if (pendingConsent != null) {
                    return@withContext false
                }

                try {
                    val ok = repository.authManager.authorizeSelectedNativeAccount(
                        selectedEmail = selectedEmail,
                        nativePlayServicesToken = playServicesToken
                    )
                    if (ok) {
                        repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    }
                    ok
                } catch (_: Throwable) {
                    false
                }
            }

            if (pendingConsent != null) {
                onRecoverableConsentIntent(pendingConsent!!)
                return@launch
            }

            if (connected) {
                val activeEmail = repository.authManager.accountState.value.email
                _signInStatusMessage.value =
                    "✓ Google Account ($activeEmail) Connected! Folder 'Android_Permanent_Backup' Ready."
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            } else {
                _signInStatusMessage.value =
                    "Could not verify Google Drive connection. Please check internet connection and tap Retry."
            }
        }
    }

    /**
     * Completes authorization after native Play Services consent sheet returns.
     */
    fun completeNativeConsentResult(context: Context, selectedEmail: String) {
        viewModelScope.launch {
            val connected = withContext(Dispatchers.IO) {
                val token = try {
                    GoogleAuthUtil.getToken(
                        context.applicationContext,
                        Account(selectedEmail, "com.google"),
                        "oauth2:${GoogleAuthManager.LEAST_PRIVILEGE_SCOPE}"
                    )
                } catch (_: Throwable) {
                    null
                }
                val ok = repository.authManager.authorizeSelectedNativeAccount(selectedEmail, token)
                if (ok) {
                    repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                }
                ok
            }
            if (connected) {
                _signInStatusMessage.value =
                    "✓ Google Account ($selectedEmail) Connected! Folder 'Android_Permanent_Backup' Ready."
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            }
        }
    }

    /**
     * 1-Tap verification and token refresh without opening any browser.
     */
    fun verifyAndRefreshCloudConnection() {
        viewModelScope.launch {
            _signInStatusMessage.value = "Verifying Google Drive connection & storage quota..."
            val ok = withContext(Dispatchers.IO) {
                try {
                    val connected = repository.authManager.connectUsingConfiguredCloudRefreshToken()
                    if (connected) {
                        repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    }
                    connected
                } catch (_: Throwable) {
                    false
                }
            }
            if (ok) {
                val email = repository.authManager.accountState.value.email
                _signInStatusMessage.value =
                    "✓ Google Drive ($email) Verified & 'Android_Permanent_Backup' Ready!"
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            } else {
                _signInStatusMessage.value = "Verification failed — please check network connection."
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
        _signInStatusMessage.value = "Google Account disconnected. Tap 'Choose Google Account' to sign in."
        refreshDashboardAndCoverage()
    }
}
