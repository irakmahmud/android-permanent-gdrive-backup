package com.permanentbackup.gdrive.ui.viewmodel

import android.app.Application
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.provider.OpenableColumns
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.BackupVersionEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.FailedOperationEntity
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
import com.permanentbackup.gdrive.worker.LocalStorageFileObserverManager
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class BackupViewModel(application: Application) : AndroidViewModel(application) {
    private val app = application as PermanentBackupApp
    private val repository = app.repository

    private val vmExceptionHandler = CoroutineExceptionHandler { _, throwable ->
        _actionFeedbackMessage.value = throwable.message ?: "Background sync operation encountered an error."
    }

    private val _dashboardSummary = MutableStateFlow(DashboardSummary())
    val dashboardSummary: StateFlow<DashboardSummary> = _dashboardSummary.asStateFlow()

    private val _folderCoverage = MutableStateFlow<List<FolderCoverageItem>>(emptyList())
    val folderCoverage: StateFlow<List<FolderCoverageItem>> = _folderCoverage.asStateFlow()

    private val _isDashboardLoading = MutableStateFlow(true)
    val isDashboardLoading: StateFlow<Boolean> = _isDashboardLoading.asStateFlow()

    private val _isScanningPhoneStorage = MutableStateFlow(true)
    val isScanningPhoneStorage: StateFlow<Boolean> = _isScanningPhoneStorage.asStateFlow()

    private val _isLoadingCloudDrive = MutableStateFlow(false)
    val isLoadingCloudDrive: StateFlow<Boolean> = _isLoadingCloudDrive.asStateFlow()

    val cloudDriveErrorMessage: StateFlow<String?> = repository.lastCloudBrowseError

    private val _isLoadingLocalBrowser = MutableStateFlow(true)
    val isLoadingLocalBrowser: StateFlow<Boolean> = _isLoadingLocalBrowser.asStateFlow()

    private val _isCreatingOrRenamingDriveFolder = MutableStateFlow(false)
    val isCreatingOrRenamingDriveFolder: StateFlow<Boolean> = _isCreatingOrRenamingDriveFolder.asStateFlow()

    private val _isAuthorizingAccount = MutableStateFlow(false)
    val isAuthorizingAccount: StateFlow<Boolean> = _isAuthorizingAccount.asStateFlow()

    private val _cloudDriveItems = MutableStateFlow<List<CloudBrowseItem>>(emptyList())
    val cloudDriveItems: StateFlow<List<CloudBrowseItem>> = _cloudDriveItems.asStateFlow()

    val cloudNextPageToken: StateFlow<String?> = repository.cloudNextPageToken

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

    private val _selectedFileVersions = MutableStateFlow<List<BackupFileEntity>>(emptyList())
    val selectedFileVersions: StateFlow<List<BackupFileEntity>> = _selectedFileVersions.asStateFlow()

    private val _accountBackupVersions = MutableStateFlow<List<BackupVersionEntity>>(emptyList())
    val accountBackupVersions: StateFlow<List<BackupVersionEntity>> = _accountBackupVersions.asStateFlow()

    private val _oauthDiagnostics = MutableStateFlow(
        OAuthDiagnosticReport(
            packageName = application.packageName,
            signingCertSha1 = "",
            signingCertSha256 = "",
            activeAccountEmail = "",
            authorizationState = com.permanentbackup.gdrive.domain.model.AccountAuthState.DISCONNECTED,
            grantedScopes = emptyList(),
            fullMyDriveReadEnabled = false,
            legacyCompromisedTokensPurged = true,
            embeddedSecretsInApk = false
        )
    )
    val oauthDiagnostics: StateFlow<OAuthDiagnosticReport> = _oauthDiagnostics.asStateFlow()

    val settings: StateFlow<BackupSettings> = repository.settings

    val activeQueue: StateFlow<List<BackupFileEntity>> = repository.observeActiveQueue()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val verifiedHistory: StateFlow<List<BackupFileEntity>> = repository.observeVerifiedHistory()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val recentRecords: StateFlow<List<BackupFileEntity>> = repository.observeRecentRecords()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val failedUploads: StateFlow<List<BackupFileEntity>> = repository.observeFailedUploads()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val syncJobs: StateFlow<List<SyncJobEntity>> = repository.observeSyncJobs()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val recentSyncEvents: StateFlow<List<SyncEventEntity>> = repository.observeRecentSyncEvents()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val unresolvedFailures: StateFlow<List<FailedOperationEntity>> = repository.observeUnresolvedFailures()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val pinnedOfflineFiles: StateFlow<List<CloudFileEntity>> = repository.observePinnedOfflineFiles()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val savedAccounts: StateFlow<List<GoogleAccountEntity>> = repository.observeGoogleAccounts()
        .catch { emit(emptyList()) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    private val fileObserverManager = LocalStorageFileObserverManager { _ ->
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            if (!settings.value.backgroundSyncPaused) {
                ensureAutoBackupRunning()
                repository.runBackupNow(respectRetryBackoff = true)
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            }
        }
    }

    @Volatile
    private var lastCoverageScanMs: Long = 0L
    @Volatile
    private var hasVerifiedDbIntegrityOnce: Boolean = false
    private var dashboardRefreshJob: kotlinx.coroutines.Job? = null

    init {
        viewModelScope.launch(vmExceptionHandler) {
            _isDashboardLoading.value = true
            _isScanningPhoneStorage.value = true
            withContext(Dispatchers.IO) {
                runCatching { repository.syncPersistedAccountOnStartup() }
                runCatching { fileObserverManager.startWatching() }
                runCatching { BackupWorkScheduler.schedulePeriodicBackup(getApplication(), settings.value) }
                runCatching { BackupWorkScheduler.scheduleContentUriTriggeredBackup(getApplication(), settings.value) }
                runCatching { ensureAutoBackupRunning() }
            }
            refreshDashboardAndCoverage(fetchRemoteQuota = true, forceCoverage = true)
            refreshLocalPhoneBrowser("")
        }

        // Real-time stream of active scan/upload activity so the UI immediately reflects the true runtime state
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            var wasRunning = false
            combine(
                repository.backupEngine.activeUploadProgress,
                repository.backupEngine.currentScanActivity,
                repository.backupEngine.isRunning,
                repository.backupEngine.isVerifyingOrCreatingDriveFolder,
                repository.authManager.accountState
            ) { progress, scanActivity, running, verifyingFolder, accountState ->
                listOf(progress, scanActivity, running, verifyingFolder, accountState)
            }.collectLatest { values ->
                val progress = values[0] as? com.permanentbackup.gdrive.domain.model.ActiveUploadProgress
                val scanActivity = values[1] as? String
                val running = values[2] as Boolean
                val verifyingFolder = values[3] as Boolean
                val accountState = values[4] as com.permanentbackup.gdrive.domain.model.GoogleAccountState

                val current = _dashboardSummary.value
                val rootFolderName = accountState.rootDriveFolderName.ifBlank { "Android_Permanent_Backup" }
                val isRootCreated = accountState.isConnected && !accountState.rootDriveFolderId.isNullOrBlank()
                val hasStoragePerm = repository.storageSource.hasStoragePermission

                val parsedFromScanPhone = scanActivity
                    ?.substringAfter("From Phone: ", "")
                    ?.substringBefore(" →", "")
                    ?.trim()
                    ?.takeIf { it.isNotBlank() }
                    ?: scanActivity
                        ?.substringAfter("Scanning Phone: ", "")
                        ?.substringBefore(" (", "")
                        ?.trim()
                        ?.takeIf { it.isNotBlank() }
                val parsedFromScanDrive = if (isRootCreated) {
                    scanActivity
                        ?.substringAfter("To Google Drive: ", "")
                        ?.trim()
                        ?.takeIf { it.isNotBlank() }
                } else {
                    null
                }

                val phoneSource = progress?.let { "/storage/emulated/0/${it.relativePath.trimStart('/')}" }
                    ?: parsedFromScanPhone
                    ?: current.currentPhoneSourcePath

                val driveTarget = if (!isRootCreated) {
                    ""
                } else {
                    progress?.let {
                        val parentFolder = it.relativePath.replace('\\', '/').substringBeforeLast('/', "")
                        if (parentFolder.isNotBlank()) {
                            "My Drive / $rootFolderName / $parentFolder / ${it.fileName}"
                        } else {
                            "My Drive / $rootFolderName / ${it.fileName}"
                        }
                    } ?: parsedFromScanDrive ?: current.currentDriveTargetPath.ifBlank { "My Drive / $rootFolderName/" }
                }

                val isAutoActive = accountState.isConnected &&
                    settings.value.userBackupConsentGranted &&
                    !settings.value.backgroundSyncPaused &&
                    hasStoragePerm

                _dashboardSummary.value = current.copy(
                    accountState = accountState,
                    isBackupRunning = running,
                    currentlyUploading = progress,
                    currentPhoneSourcePath = phoneSource,
                    currentDriveTargetPath = driveTarget,
                    currentScanningOrUploadingFile = scanActivity ?: current.currentScanningOrUploadingFile,
                    isContinuousAutoBackupServiceActive = isAutoActive,
                    isScanningPhoneStorage = _isScanningPhoneStorage.value || running,
                    isVerifyingOrCreatingDriveFolder = verifyingFolder || _isCreatingOrRenamingDriveFolder.value,
                    hasStoragePermission = hasStoragePerm,
                    hasManageAllFilesPermission = repository.storageSource.hasManageAllFilesPermission(),
                    isBatteryOptimizationIgnored = repository.isBatteryOptimizationIgnored(),
                    isDriveRootFolderCreated = isRootCreated,
                    watchedFolderCount = fileObserverManager.getWatchedDirectoryPaths().size
                )

                if (wasRunning && !running) {
                    refreshDashboardAndCoverage(fetchRemoteQuota = false, forceCoverage = true)
                }
                wasRunning = running
            }
        }
    }

    fun ensureAutoBackupRunning() {
        runCatching {
            if (!settings.value.userBackupConsentGranted) {
                viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
                    repository.saveSettingsToDatabase(settings.value.copy(userBackupConsentGranted = true))
                }
            }
            if (!settings.value.backgroundSyncPaused &&
                (repository.authManager.accountState.value.isConnected || repository.storageSource.hasStoragePermission)
            ) {
                PermanentBackupForegroundService.startManualBackupService(getApplication())
            }
        }
    }

    fun onStoragePermissionsGranted() {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            runCatching { fileObserverManager.startWatching() }
            runCatching { ensureAutoBackupRunning() }
            refreshDashboardAndCoverage(fetchRemoteQuota = false, forceCoverage = true)
            refreshLocalPhoneBrowser(_currentLocalPath.value)
        }
    }

    fun onAppResumed() {
        onStoragePermissionsGranted()
    }

    override fun onCleared() {
        fileObserverManager.stopWatching()
        super.onCleared()
    }

    fun refreshDashboardAndCoverage(
        fetchRemoteQuota: Boolean = false,
        forceCoverage: Boolean = false
    ) {
        if (!fetchRemoteQuota && !forceCoverage && dashboardRefreshJob?.isActive == true) {
            return
        }
        dashboardRefreshJob = viewModelScope.launch(vmExceptionHandler) {
            withContext(Dispatchers.IO) {
                if (fetchRemoteQuota && repository.authManager.accountState.value.isConnected) {
                    runCatching { repository.backupEngine.refreshRemoteQuotaAndRootFolder() }
                    runCatching { repository.backupEngine.syncCloudChangesIncremental() }
                    runCatching { repository.persistCurrentAccountToDatabase() }
                }
                val dbOk = if (!hasVerifiedDbIntegrityOnce) {
                    hasVerifiedDbIntegrityOnce = true
                    runCatching {
                        app.database.checkDatabaseIntegrity() || app.database.attemptNonDestructiveRecovery()
                    }.getOrDefault(true)
                } else {
                    _dashboardSummary.value.dbIntegrityOk
                }
                val now = System.currentTimeMillis()
                val shouldScanCoverage = forceCoverage ||
                    _folderCoverage.value.isEmpty() ||
                    now - lastCoverageScanMs > 15_000L
                if (shouldScanCoverage) {
                    _isScanningPhoneStorage.value = true
                }
                val coverage = if (shouldScanCoverage) {
                    lastCoverageScanMs = now
                    try {
                        runCatching { repository.getFolderCoverageItems() }.getOrDefault(_folderCoverage.value)
                    } finally {
                        _isScanningPhoneStorage.value = false
                    }
                } else {
                    _folderCoverage.value
                }
                val watchedCount = fileObserverManager.getWatchedDirectoryPaths().size
                val summary = runCatching {
                    repository.buildDashboardSummary(
                        precomputedCoverage = coverage,
                        isDashboardLoading = false,
                        isScanningPhoneStorage = _isScanningPhoneStorage.value,
                        watchedFolderCountOverride = watchedCount
                    ).copy(dbIntegrityOk = dbOk)
                }.getOrDefault(DashboardSummary(dbIntegrityOk = dbOk, isDashboardLoading = false))
                val versions = runCatching {
                    repository.getAccountBackupVersionsPaged(limit = 200)
                }.getOrDefault(emptyList())
                val diag = runCatching { repository.getOAuthDiagnostics() }.getOrDefault(_oauthDiagnostics.value)

                _folderCoverage.value = coverage
                _dashboardSummary.value = summary
                _isDashboardLoading.value = false
                _accountBackupVersions.value = versions
                _oauthDiagnostics.value = diag
            }
            if (fetchRemoteQuota) {
                refreshCloudDriveExplorer()
            }
        }
    }

    fun refreshCloudDriveExplorer() {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _isLoadingCloudDrive.value = true
            try {
                val currentFolderId = _driveFolderBreadcrumbs.value.lastOrNull()?.first
                val items = repository.browseCloudDriveFiles(
                    filter = _driveFilter.value,
                    parentFolderId = currentFolderId,
                    searchQuery = _driveSearchQuery.value,
                    sortOrder = _driveSortOrder.value,
                    pageToken = null
                )
                _cloudDriveItems.value = items
            } finally {
                _isLoadingCloudDrive.value = false
            }
        }
    }

    fun loadMoreCloudDriveFiles() {
        val nextToken = cloudNextPageToken.value ?: return
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _isLoadingCloudDrive.value = true
            try {
                val currentFolderId = _driveFolderBreadcrumbs.value.lastOrNull()?.first
                val nextItems = repository.browseCloudDriveFiles(
                    filter = _driveFilter.value,
                    parentFolderId = currentFolderId,
                    searchQuery = _driveSearchQuery.value,
                    sortOrder = _driveSortOrder.value,
                    pageToken = nextToken
                )
                val existingIds = _cloudDriveItems.value.map { it.driveFileId }.toSet()
                _cloudDriveItems.value = _cloudDriveItems.value + nextItems.filter { it.driveFileId !in existingIds }
            } finally {
                _isLoadingCloudDrive.value = false
            }
        }
    }

    fun createNewCloudFolder(folderName: String) {
        if (folderName.isBlank()) return
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _isLoadingCloudDrive.value = true
            try {
                val currentFolderId = _driveFolderBreadcrumbs.value.lastOrNull()?.first
                val createdId = repository.createCloudFolder(folderName.trim(), currentFolderId)
                if (createdId != null) {
                    _actionFeedbackMessage.value = "✓ Folder '${folderName.trim()}' is created on Google Drive."
                    refreshCloudDriveExplorer()
                } else {
                    _actionFeedbackMessage.value = "Could not create folder '${folderName.trim()}' on Google Drive. Connect your Google Account first."
                }
            } finally {
                _isLoadingCloudDrive.value = false
            }
        }
    }

    /**
     * Allows the user to explicitly create the root backup folder on Google Drive right now,
     * or rename / create a new target Google Drive backup folder.
     */
    fun createOrRenameRootBackupFolder(newFolderName: String) {
        val cleanName = newFolderName.trim()
            .replace("/", "_")
            .replace("\\", "_")
            .ifBlank { "Android_Permanent_Backup" }
        viewModelScope.launch(vmExceptionHandler) {
            _isCreatingOrRenamingDriveFolder.value = true
            _actionFeedbackMessage.value = if (repository.authManager.accountState.value.isConnected) {
                "Creating / verifying folder '$cleanName' on Google Drive... Please wait..."
            } else {
                "Saving target backup folder name '$cleanName'..."
            }
            val result = withContext(Dispatchers.IO) {
                repository.createOrRenameRootBackupFolder(cleanName)
            }
            _isCreatingOrRenamingDriveFolder.value = false
            result.fold(
                onSuccess = { folderId ->
                    if (folderId.isNotBlank()) {
                        _actionFeedbackMessage.value =
                            "✓ Folder '$cleanName' is created on Google Drive (My Drive/$cleanName, ID: $folderId)."
                        _signInStatusMessage.value =
                            "✓ Google Drive folder '$cleanName' is created at My Drive/$cleanName."
                    } else {
                        _actionFeedbackMessage.value =
                            "Target folder set to '$cleanName'. Folder is not created on Google Drive yet — connect your Google Account to create it."
                    }
                    refreshDashboardAndCoverage(fetchRemoteQuota = false, forceCoverage = true)
                    refreshCloudDriveExplorer()
                },
                onFailure = { err ->
                    _actionFeedbackMessage.value =
                        "Could not create folder '$cleanName' on Google Drive: ${err.message}"
                    refreshDashboardAndCoverage(fetchRemoteQuota = false)
                }
            )
        }
    }

    /**
     * @param targetDriveFolderId upload straight into this exact Google Drive folder (My Drive "Upload File")
     * @param stageTemporaryGrant share-sheet URIs carry a temporary grant -> stage bytes app-privately first
     */
    fun uploadDirectUrisToDrive(
        uris: List<Uri>,
        targetSubfolder: String = "Direct_Uploads",
        targetDriveFolderId: String? = null,
        stageTemporaryGrant: Boolean = false
    ) {
        if (uris.isEmpty()) return
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            if (!settings.value.userBackupConsentGranted) {
                repository.saveSettingsToDatabase(settings.value.copy(userBackupConsentGranted = true))
            }
            val resolver = getApplication<Application>().contentResolver
            var queuedCount = 0
            for (uri in uris) {
                var displayName = uri.lastPathSegment?.substringAfterLast('/') ?: "upload_${System.currentTimeMillis()}"
                var sizeBytes = 0L
                try {
                    resolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE), null, null, null)?.use { c ->
                        if (c.moveToFirst()) {
                            val nameIdx = c.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                            val sizeIdx = c.getColumnIndex(OpenableColumns.SIZE)
                            if (nameIdx >= 0 && !c.isNull(nameIdx)) displayName = c.getString(nameIdx)
                            if (sizeIdx >= 0 && !c.isNull(sizeIdx)) sizeBytes = c.getLong(sizeIdx)
                        }
                    }
                } catch (_: Throwable) {
                }
                val mimeType = try {
                    resolver.getType(uri) ?: "application/octet-stream"
                } catch (_: Throwable) {
                    "application/octet-stream"
                }
                val queued = repository.uploadDirectUriToDrive(
                    uriString = uri.toString(),
                    displayName = displayName,
                    mimeType = mimeType,
                    sizeBytes = sizeBytes,
                    targetSubfolder = targetSubfolder,
                    targetDriveFolderId = targetDriveFolderId,
                    stageTemporaryGrant = stageTemporaryGrant
                )
                if (queued) queuedCount++
            }
            if (queuedCount > 0) {
                _actionFeedbackMessage.value = "Queued $queuedCount file(s) to Permanent Drive ($targetSubfolder)."
                // Upload-only run: starts immediately without a full phone storage rescan
                PermanentBackupForegroundService.startUploadQueueService(getApplication())
            } else {
                _actionFeedbackMessage.value = "Selected file(s) already backed up and unchanged."
            }
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun onSafFolderGranted(treeUri: Uri) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            val label = treeUri.lastPathSegment?.substringAfterLast(':')?.substringAfterLast('/')?.ifBlank { "SAF_Custom" }
                ?: "SAF_Custom"
            repository.registerPersistedSafFolder(treeUri.toString(), label)
            _actionFeedbackMessage.value = "Added SAF folder '$label' to permanent auto-backup."
            refreshDashboardAndCoverage()
            PermanentBackupForegroundService.startManualBackupService(getApplication())
        }
    }

    fun toggleLocalSubfolderAutoBackup(item: LocalBrowseItem, enabled: Boolean) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.toggleCustomLocalFolderAutoBackup(item, enabled)
            _actionFeedbackMessage.value = if (enabled) {
                "Added subfolder '${item.relativePath}' to auto-backup."
            } else {
                "Excluded subfolder '${item.relativePath}' from new backups (cloud files untouched)."
            }
            refreshLocalPhoneBrowser(_currentLocalPath.value)
            refreshDashboardAndCoverage()
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
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _isLoadingLocalBrowser.value = true
            try {
                _currentLocalPath.value = relativeSubPath
                _localPhoneItems.value = repository.browseLocalPhoneStorage(relativeSubPath)
            } finally {
                _isLoadingLocalBrowser.value = false
            }
        }
    }

    fun navigateUpLocalFolder() {
        val curr = _currentLocalPath.value.trim('/')
        val parent = if (curr.contains('/')) curr.substringBeforeLast('/') else ""
        refreshLocalPhoneBrowser(parent)
    }

    fun toggleFolderBackup(item: FolderCoverageItem, enabled: Boolean) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
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
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            if (!settings.value.userBackupConsentGranted) {
                repository.saveSettingsToDatabase(settings.value.copy(userBackupConsentGranted = true))
            }
            val queued = repository.queueSingleLocalFile(item)
            if (queued) {
                _actionFeedbackMessage.value = "Queued '${item.name}' for permanent Google Drive backup."
                repository.runUploadQueueNow()
            } else {
                _actionFeedbackMessage.value = "'${item.name}' is already backed up and unchanged."
            }
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun triggerManualBackupNow() {
        val hasStoragePerm = repository.storageSource.hasStoragePermission
        val accountState = repository.authManager.accountState.value
        if (!hasStoragePerm) {
            _actionFeedbackMessage.value =
                "Storage permission is required to scan phone files. Please tap 'Grant All Files Access' first."
            refreshDashboardAndCoverage(fetchRemoteQuota = false)
            return
        }
        // PermanentBackupForegroundService executes app.repository.runBackupNow() on serviceScope;
        // do not duplicate the runBackupNow() call from ViewModel.
        PermanentBackupForegroundService.startManualBackupService(getApplication())
        _actionFeedbackMessage.value = if (!accountState.isConnected) {
            "Scanning phone folders locally... Connect your Google Account to create folder '${accountState.rootDriveFolderName}' and upload."
        } else if (accountState.rootDriveFolderId.isNullOrBlank()) {
            "Scanning phone files & creating folder '${accountState.rootDriveFolderName}' on Google Drive... Please wait."
        } else {
            "Scanning phone folders & syncing to Google Drive folder '${accountState.rootDriveFolderName}'... Please wait."
        }
        refreshDashboardAndCoverage(fetchRemoteQuota = false)
    }

    fun togglePauseAllTransfers() {
        val current = settings.value
        val nextPaused = !current.backgroundSyncPaused
        updateSettings(current.copy(backgroundSyncPaused = nextPaused))
    }

    fun togglePauseSingleRecord(recordId: Long, pause: Boolean) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.backupEngine.pauseSingleRecord(recordId, pause)
            refreshDashboardAndCoverage()
            if (!pause) {
                // Resume = upload immediately (backoff cleared); joins an in-flight cycle if one is running
                repository.runUploadQueueNow()
                refreshDashboardAndCoverage()
            }
        }
    }

    fun retryFailedUploads() {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.retryAllFailedUploads()
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun retrySingleRecord(recordId: Long) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.requeueFailedRecord(recordId)
            repository.runUploadQueueNow()
            refreshDashboardAndCoverage(fetchRemoteQuota = true)
        }
    }

    fun restoreBackupVersion(record: BackupFileEntity) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _actionFeedbackMessage.value = "Restoring '${record.driveStoredName}' (v${record.backupVersion})..."
            val outcome = repository.restoreBackupRecord(record)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun restoreAccountBackupVersionEntity(ver: BackupVersionEntity) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _actionFeedbackMessage.value = "Restoring '${ver.driveStoredName}' (v${ver.versionNumber})..."
            val outcome = repository.restoreBackupVersionEntity(ver)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun inspectAllVersionsForFile(sourceFileIdentity: String) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _selectedFileVersions.value = repository.getAllVersionsForFile(sourceFileIdentity)
        }
    }

    fun dismissVersionsDialog() {
        _selectedFileVersions.value = emptyList()
    }

    fun restoreCloudFile(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _actionFeedbackMessage.value = if (item.isFolder) {
                "Downloading folder '${item.name}' recursively from Google Drive..."
            } else {
                "Downloading '${item.name}' from Google Drive..."
            }
            val outcome = repository.restoreCloudBrowseItem(item)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun mirrorCloudFolderToPhone(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            _actionFeedbackMessage.value = "Mirroring folder '${item.name}' (One-Way Cloud -> Phone, Isolated from Backup)..."
            val outcome = repository.mirrorCloudFolderToLocal(item)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun toggleCloudFileOfflinePin(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            val outcome = repository.toggleOfflinePin(item)
            _actionFeedbackMessage.value = outcome.message
            refreshDashboardAndCoverage()
        }
    }

    fun unpinOfflineEntity(entity: CloudFileEntity) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.cloudFileOps.unpinCloudFileOffline(entity.driveFileId)
            _actionFeedbackMessage.value = "Unpinned '${entity.name}' from local offline cache."
            refreshDashboardAndCoverage()
        }
    }

    fun previewCloudFileStream(item: CloudBrowseItem) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            val snippet = repository.cloudFileOps.streamPreviewSnippet(
                driveFileId = item.driveFileId,
                workspaceMimeType = item.mimeType
            )
            _previewSnippet.value = item.name to snippet
        }
    }

    fun dismissPreviewDialog() {
        _previewSnippet.value = null
    }

    fun updateSettings(newSettings: BackupSettings) {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.saveSettingsToDatabase(newSettings)
            BackupWorkScheduler.schedulePeriodicBackup(getApplication(), newSettings)
            if (!newSettings.backgroundSyncPaused) {
                PermanentBackupForegroundService.startManualBackupService(getApplication())
            }
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
        skipGisResolution: Boolean = false,
        gisAccessToken: String? = null,
        onResolutionPendingIntent: (PendingIntent) -> Unit = {},
        onRecoverableConsentIntent: (Intent) -> Unit = {}
    ) {
        viewModelScope.launch(vmExceptionHandler) {
            _isAuthorizingAccount.value = true
            _signInStatusMessage.value = "Loading... Authorizing Google Drive access for $selectedEmail..."
            val outcome = withContext(Dispatchers.IO) {
                val res = repository.authManager.trySilentOrInteractiveAuthorization(
                    context = context,
                    selectedEmail = selectedEmail,
                    requestFullDriveRead = requestFullDriveRead,
                    skipGisResolution = skipGisResolution,
                    gisAccessToken = gisAccessToken
                )
                if (res.authorized) {
                    repository.backupEngine.refreshRemoteQuotaAndRootFolder()
                    repository.persistCurrentAccountToDatabase()
                }
                res
            }
            _isAuthorizingAccount.value = false

            if (!skipGisResolution && outcome.resolutionPendingIntent != null) {
                onResolutionPendingIntent(outcome.resolutionPendingIntent)
                return@launch
            }
            if (outcome.recoverableConsentIntent != null) {
                onRecoverableConsentIntent(outcome.recoverableConsentIntent)
                return@launch
            }

            if (outcome.authorized) {
                val state = repository.authManager.accountState.value
                val folderName = state.rootDriveFolderName.ifBlank { "Android_Permanent_Backup" }
                val folderStatus = if (!state.rootDriveFolderId.isNullOrBlank()) {
                    "✓ Folder '$folderName' is created on Google Drive (My Drive/$folderName)."
                } else {
                    "Folder '$folderName' is not created on Google Drive yet (it will be created when backup runs, or tap 'Create Folder Now' / 'Rename' to create it now)."
                }
                _signInStatusMessage.value =
                    "✓ Google Account (${state.email}) Connected! $folderStatus"
                ensureAutoBackupRunning()
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
                refreshCloudDriveExplorer()
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
        requestFullDriveRead: Boolean = false,
        gisAccessToken: String? = null,
        onRecoverableConsentIntent: (Intent) -> Unit = {}
    ) {
        onNativeGoogleAccountSelected(
            context = context,
            selectedEmail = selectedEmail,
            requestFullDriveRead = requestFullDriveRead,
            skipGisResolution = true,
            gisAccessToken = gisAccessToken,
            onRecoverableConsentIntent = onRecoverableConsentIntent
        )
    }

    fun switchActiveSavedAccount(context: Context, email: String) {
        onNativeGoogleAccountSelected(context = context, selectedEmail = email)
    }

    fun verifyAndRefreshCloudConnection() {
        viewModelScope.launch(vmExceptionHandler) {
            _isAuthorizingAccount.value = true
            _signInStatusMessage.value = "Loading... Verifying Google Drive connection, quota & backup folder..."
            val ok = withContext(Dispatchers.IO) {
                if (!repository.authManager.accountState.value.isConnected) {
                    val targetEmail = repository.authManager.accountState.value.email.trim()
                    if (targetEmail.isNotBlank()) {
                        repository.authManager.authorizeSelectedNativeAccount(selectedEmail = targetEmail)
                    } else {
                        repository.authManager.authorizeViaCloudDriveBridge("")
                    }
                }
                val quotaOk = repository.backupEngine.refreshRemoteQuotaAndRootFolder(createFolderIfMissing = true)
                if (quotaOk) {
                    repository.persistCurrentAccountToDatabase()
                }
                quotaOk
            }
            _isAuthorizingAccount.value = false
            val state = repository.authManager.accountState.value
            val folderName = state.rootDriveFolderName.ifBlank { "Android_Permanent_Backup" }
            if (ok && !state.rootDriveFolderId.isNullOrBlank()) {
                _signInStatusMessage.value =
                    "✓ Google Drive (${state.email}) Verified! Folder '$folderName' is created at My Drive/$folderName (ID: ${state.rootDriveFolderId})."
                ensureAutoBackupRunning()
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
                refreshCloudDriveExplorer()
            } else if (state.isConnected) {
                _signInStatusMessage.value =
                    "Connected as ${state.email}, but folder '$folderName' is not created yet: ${repository.backupEngine.lastNetworkError.value ?: "Tap 'Create Folder Now' to retry."}"
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            } else {
                _signInStatusMessage.value =
                    "Google Account is not connected. Tap 'Connect Google Account' to sign in."
                refreshDashboardAndCoverage(fetchRemoteQuota = false)
            }
        }
    }

    fun disconnectGoogleAccount() {
        viewModelScope.launch(Dispatchers.IO + vmExceptionHandler) {
            repository.authManager.disconnectAccount()
            repository.persistCurrentAccountToDatabase()
            _signInStatusMessage.value = "Signed out. Tap 'Connect Google Account' when you wish to sign in."
            refreshDashboardAndCoverage()
        }
    }
}
