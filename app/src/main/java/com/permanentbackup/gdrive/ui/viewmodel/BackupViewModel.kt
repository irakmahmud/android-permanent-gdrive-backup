package com.permanentbackup.gdrive.ui.viewmodel

import android.app.Application
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.GoogleAccountEntity
import com.permanentbackup.gdrive.data.local.SyncEventEntity
import com.permanentbackup.gdrive.data.local.SyncJobEntity
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.DriveSortOrder
import com.permanentbackup.gdrive.domain.model.DriveViewFilter
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.LocalBrowseItem
import com.permanentbackup.gdrive.domain.model.OAuthDiagnosticReport
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

    private val _cloudDriveItems = MutableStateFlow<List<CloudBrowseItem>>(emptyList())
    val cloudDriveItems: StateFlow<List<CloudBrowseItem>> = _cloudDriveItems.asStateFlow()

    private val _driveFilter = MutableStateFlow(DriveViewFilter.BACKUP_ROOT)
    val driveFilter: StateFlow<DriveViewFilter> = _driveFilter.asStateFlow()

    private val _driveSortOrder = MutableStateFlow(DriveSortOrder.MODIFIED_DESC)
    val driveSortOrder: StateFlow<DriveSortOrder> = _driveSortOrder.asStateFlow()

    private val _driveFolderBreadcrumbs = MutableStateFlow<List<Pair<String, String>>>(emptyList())
    val driveFolderBreadcrumbs: StateFlow<List<Pair<String, String>>> = _driveFolderBreadcrumbs.asStateFlow()

    private val _driveSearchQuery = MutableStateFlow("")
    val driveSearchQuery: StateFlow<String> = _driveSearchQuery.asStateFlow()

    private val _isDriveGridView = MutableStateFlow(false)
    val isDriveGridView: StateFlow<Boolean> = _isDriveGridView.asStateFlow()

    private val _localPhoneItems = MutableStateFlow<List<LocalBrowseItem>>(emptyList())
    val localPhoneItems: StateFlow<List<LocalBrowseItem>> = _localPhoneItems.asStateFlow()

    private val _currentLocalPath = MutableStateFlow("")
    val currentLocalPath: StateFlow<String> = _currentLocalPath.asStateFlow()

    private val _signInStatusMessage = MutableStateFlow<String?>(null)
    val signInStatusMessage: StateFlow<String?> = _signInStatusMessage.asStateFlow()

    private val _actionFeedbackMessage = MutableStateFlow<String?>(null)
    val actionFeedbackMessage: StateFlow<String?> = _actionFeedbackMessage.asStateFlow()

    private val _previewSnippet = MutableStateFlow<Pair<String, String>?>(null)
    val previewSnippet: StateFlow<Pair<String, String>?> = _previewSnippet.asStateFlow()

    private val _oauthDiagnostics = MutableStateFlow(repository.getOAuthDiagnostics())
    val oauthDiagnostics: StateFlow<OAuthDiagnosticReport> = _oauthDiagnostics.asStateFlow()

    val settings: StateFlow<BackupSettings> = repository.settings

    val activeQueue: StateFlow<List<BackupFileEntity>> = repository.observeActiveQueue()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val verifiedHistory: StateFlow<List<BackupFileEntity>> = repository.observeVerifiedHistory()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val recentRecords: StateFlow<List<BackupFileEntity>> = repository.observeRecentRecords()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val failedUploads: StateFlow<List<BackupFileEntity>> = repository.observeFailedUploads()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val syncJobs: StateFlow<List<SyncJobEntity>> = repository.observeSyncJobs()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val recentSyncEvents: StateFlow<List<SyncEventEntity>> = repository.observeRecentSyncEvents()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val pinnedOfflineFiles: StateFlow<List<CloudFileEntity>> = repository.observePinnedOfflineFiles()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val savedAccounts: StateFlow<List<GoogleAccountEntity>> = repository.observeGoogleAccounts()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    init {
        viewModelScope.launch {
            withContext(Dispatchers.IO) {
                repository.syncPersistedAccountOnStartup()
            }
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
            refreshLocalPhoneBrowser("")
        }
    }

    fun refreshDashboardAndCoverage(fetchRemoteQuota: Boolean = false) {
        viewModelScope.launch {
            if (fetchRemoteQuota && repository.authManager.accountState.value.isConnected) {
                withContext(Dispatchers.IO) {
                    repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    repository.backupEngine.syncCloudChangesIncremental()
                    repository.persistCurrentAccountToDatabase()
                }
            }
            val dbOk = withContext(Dispatchers.IO) {
                app.database.checkDatabaseIntegrity() || app.database.attemptNonDestructiveRecovery()
            }
            _dashboardSummary.value = repository.buildDashboardSummary().copy(dbIntegrityOk = dbOk)
            _folderCoverage.value = repository.getFolderCoverageItems()
            _oauthDiagnostics.value = repository.getOAuthDiagnostics()
            refreshCloudDriveExplorer()
        }
    }

    fun refreshCloudDriveExplorer() {
        viewModelScope.launch(Dispatchers.IO) {
            val currentFolderId = _driveFolderBreadcrumbs.value.lastOrNull()?.first
            val items = repository.browseCloudDriveFiles(
                filter = _driveFilter.value,
                parentFolderId = currentFolderId,
                searchQuery = _driveSearchQuery.value,
                sortOrder = _driveSortOrder.value
            )
            _cloudDriveItems.value = items
        }
    }

    fun setDriveFilter(filter: DriveViewFilter) {
        _driveFilter.value = filter
        _driveFolderBreadcrumbs.value = emptyList()
        refreshCloudDriveExplorer()
    }

    fun setDriveSortOrder(sortOrder: DriveSortOrder) {
        _driveSortOrder.value = sortOrder
        refreshCloudDriveExplorer()
    }

    fun setDriveSearchQuery(query: String) {
        _driveSearchQuery.value = query
        refreshCloudDriveExplorer()
    }

    fun toggleDriveGridView() {
        _isDriveGridView.value = !_isDriveGridView.value
    }

    fun openDriveSubfolder(folderId: String, folderName: String) {
        _driveFolderBreadcrumbs.value = _driveFolderBreadcrumbs.value + (folderId to folderName)
        refreshCloudDriveExplorer()
    }

    fun navigateUpDriveFolder(targetIndex: Int = -1) {
        val current = _driveFolderBreadcrumbs.value
        _driveFolderBreadcrumbs.value = if (targetIndex < 0) {
            emptyList()
        } else {
            current.take(targetIndex + 1)
        }
        refreshCloudDriveExplorer()
    }

    fun refreshLocalPhoneBrowser(relativeSubPath: String = _currentLocalPath.value) {
        viewModelScope.launch(Dispatchers.IO) {
            _currentLocalPath.value = relativeSubPath
            _localPhoneItems.value = repository.browseLocalPhoneStorage(relativeSubPath)
        }
    }

    fun navigateUpLocalFolder() {
        val curr = _currentLocalPath.value.trim('/')
        val parent = if (curr.contains('/')) curr.substringBeforeLast('/') else ""
        refreshLocalPhoneBrowser(parent)
    }

    fun toggleFolderBackup(item: FolderCoverageItem, enabled: Boolean) {
        viewModelScope.launch(Dispatchers.IO) {
            repository.toggleFolderBackupEnabled(item, enabled)
            _actionFeedbackMessage.value = if (enabled) {
                "Enabled permanent backup for folder '${item.folderName}'."
            } else {
                "Paused new backups for folder '${item.folderName}' (existing cloud files preserved)."
            }
            refreshDashboardAndCoverage()
        }
    }

    fun backupSingleLocalFile(item: LocalBrowseItem) {
        viewModelScope.launch(Dispatchers.IO) {
            val queued = repository.queueSingleLocalFile(item)
            if (queued) {
                _actionFeedbackMessage.value = "Queued '${item.name}' for permanent Google Drive backup."
                repository.runBackupNow()
            } else {
                _actionFeedbackMessage.value = "'${item.name}' is already backed up and unchanged."
            }
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun triggerManualBackupNow() {
        PermanentBackupForegroundService.startManualBackupService(getApplication())
        viewModelScope.launch(Dispatchers.IO) {
            repository.runBackupNow()
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun togglePauseAllTransfers() {
        val current = settings.value
        val nextPaused = !current.backgroundSyncPaused
        updateSettings(current.copy(backgroundSyncPaused = nextPaused))
    }

    fun togglePauseSingleRecord(recordId: Long, pause: Boolean) {
        viewModelScope.launch(Dispatchers.IO) {
            repository.backupEngine.pauseSingleRecord(recordId, pause)
            refreshDashboardAndCoverage()
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

    fun restoreBackupVersion(record: BackupFileEntity) {
        viewModelScope.launch(Dispatchers.IO) {
            _actionFeedbackMessage.value = "Restoring '${record.driveStoredName}' (v${record.backupVersion})..."
            val outcome = repository.restoreBackupRecord(record)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun restoreCloudFile(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO) {
            _actionFeedbackMessage.value = "Downloading '${item.name}' from Google Drive..."
            val outcome = repository.restoreCloudBrowseItem(item)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun toggleCloudFileOfflinePin(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO) {
            val outcome = repository.toggleOfflinePin(item)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun unpinOfflineEntity(entity: CloudFileEntity) {
        viewModelScope.launch(Dispatchers.IO) {
            repository.cloudFileOps.unpinCloudFileOffline(entity.driveFileId)
            _actionFeedbackMessage.value = "Unpinned '${entity.name}' from local offline cache."
            refreshDashboardAndCoverage()
        }
    }

    fun previewCloudFileStream(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO) {
            val snippet = repository.cloudFileOps.streamPreviewSnippet(item.driveFileId)
            _previewSnippet.value = item.name to snippet
        }
    }

    fun dismissPreviewDialog() {
        _previewSnippet.value = null
    }

    fun updateSettings(newSettings: BackupSettings) {
        viewModelScope.launch(Dispatchers.IO) {
            repository.saveSettingsToDatabase(newSettings)
            BackupWorkScheduler.schedulePeriodicBackup(getApplication(), newSettings)
            refreshDashboardAndCoverage()
        }
    }

    /**
     * Section 3: Official Google Identity Services AuthorizationClient + Play Services Silent Flow.
     * Separates Account Selection from Drive Scope Authorization.
     */
    fun onNativeGoogleAccountSelected(
        context: Context,
        selectedEmail: String,
        requestFullDriveRead: Boolean = false,
        onResolutionPendingIntent: (PendingIntent) -> Unit = {},
        onRecoverableConsentIntent: (Intent) -> Unit = {}
    ) {
        viewModelScope.launch {
            _signInStatusMessage.value = "Authorizing Google Drive access for $selectedEmail..."
            val outcome = withContext(Dispatchers.IO) {
                val res = repository.authManager.trySilentOrInteractiveAuthorization(
                    context = context,
                    selectedEmail = selectedEmail,
                    requestFullDriveRead = requestFullDriveRead
                )
                if (res.authorized) {
                    repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    repository.persistCurrentAccountToDatabase()
                }
                res
            }

            if (outcome.resolutionPendingIntent != null) {
                onResolutionPendingIntent(outcome.resolutionPendingIntent)
                return@launch
            }
            if (outcome.recoverableConsentIntent != null) {
                onRecoverableConsentIntent(outcome.recoverableConsentIntent)
                return@launch
            }

            if (outcome.authorized) {
                val activeEmail = repository.authManager.accountState.value.email
                _signInStatusMessage.value =
                    "✓ Google Account ($activeEmail) Authorized! Persistent Session & 'Android_Permanent_Backup' Active."
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            } else {
                _signInStatusMessage.value = outcome.errorMessage
                    ?: "Google Drive authorization requires consent or OAuth client registration."
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            }
        }
    }

    fun completeNativeConsentResult(
        context: Context,
        selectedEmail: String,
        requestFullDriveRead: Boolean = false
    ) {
        onNativeGoogleAccountSelected(
            context = context,
            selectedEmail = selectedEmail,
            requestFullDriveRead = requestFullDriveRead
        )
    }

    fun switchActiveSavedAccount(context: Context, email: String) {
        onNativeGoogleAccountSelected(context = context, selectedEmail = email)
    }

    fun verifyAndRefreshCloudConnection() {
        viewModelScope.launch {
            _signInStatusMessage.value = "Verifying Google Drive connection & storage quota..."
            val ok = withContext(Dispatchers.IO) {
                repository.backupEngine.refreshRemoteQuotaAndRootFolder()
            }
            if (ok) {
                val email = repository.authManager.accountState.value.email
                _signInStatusMessage.value =
                    "✓ Google Drive ($email) Verified & 'Android_Permanent_Backup' Ready!"
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            } else {
                _signInStatusMessage.value =
                    "Could not reach Google Drive or authorization needed. Check internet or reconnect."
            }
        }
    }

    fun disconnectGoogleAccount() {
        viewModelScope.launch(Dispatchers.IO) {
            repository.authManager.disconnectAccount()
            repository.persistCurrentAccountToDatabase()
            _signInStatusMessage.value = "Signed out. Tap 'Connect Google Account' when you wish to sign in."
            refreshDashboardAndCoverage()
        }
    }
}
