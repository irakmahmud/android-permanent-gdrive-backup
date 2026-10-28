package com.permanentbackup.gdrive.data.repository

import android.content.Context
import android.content.SharedPreferences
import com.permanentbackup.gdrive.data.local.AppSettingsEntity
import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.BackupFolderEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.GoogleAccountEntity
import com.permanentbackup.gdrive.data.local.SyncEventEntity
import com.permanentbackup.gdrive.data.local.SyncJobEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.engine.BackupCycleSummary
import com.permanentbackup.gdrive.domain.engine.CloudFileOperationsManager
import com.permanentbackup.gdrive.domain.engine.PermanentBackupEngine
import com.permanentbackup.gdrive.domain.engine.StorageFileSource
import com.permanentbackup.gdrive.domain.model.AccountAuthState
import com.permanentbackup.gdrive.domain.model.BackgroundSyncStatus
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.DriveSortOrder
import com.permanentbackup.gdrive.domain.model.DriveViewFilter
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.LocalBrowseItem
import com.permanentbackup.gdrive.domain.model.OAuthDiagnosticReport
import com.permanentbackup.gdrive.domain.model.RestoreOutcome
import com.permanentbackup.gdrive.domain.model.UploadStatus
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

class BackupRepository(
    private val context: Context? = null,
    val backupDao: BackupDao,
    val storageSource: StorageFileSource,
    val driveApiClient: DriveApiClient,
    val authManager: GoogleAuthManager,
    customSettingsPrefs: SharedPreferences? = null
) {
    private val prefs: SharedPreferences? = customSettingsPrefs
        ?: context?.applicationContext?.getSharedPreferences("permanent_backup_settings", Context.MODE_PRIVATE)

    val backupEngine = PermanentBackupEngine(
        backupDao = backupDao,
        storageSource = storageSource,
        driveApiClient = driveApiClient,
        authManager = authManager
    )

    val cloudFileOps = CloudFileOperationsManager(
        context = context,
        backupDao = backupDao,
        driveApiClient = driveApiClient,
        authManager = authManager
    )

    private val _settings = MutableStateFlow(loadSettings())
    val settings: StateFlow<BackupSettings> = _settings.asStateFlow()

    private val _nextScheduledBackupMs = MutableStateFlow<Long?>(
        System.currentTimeMillis() + (6L * 3600L * 1000L)
    )
    val nextScheduledBackupMs: StateFlow<Long?> = _nextScheduledBackupMs.asStateFlow()

    fun observeActiveQueue(): Flow<List<BackupFileEntity>> = backupDao.observeActiveQueue()
    fun observeVerifiedHistory(): Flow<List<BackupFileEntity>> = backupDao.observeVerifiedHistory()
    fun observeFailedUploads(): Flow<List<BackupFileEntity>> = backupDao.observeFailedRecords()
    fun observeRecentRecords(): Flow<List<BackupFileEntity>> = backupDao.observeRecentRecords()
    fun observeSyncJobs(): Flow<List<SyncJobEntity>> = backupDao.observeSyncJobs()
    fun observeRecentSyncEvents(): Flow<List<SyncEventEntity>> = backupDao.observeRecentSyncEvents()
    fun observeGoogleAccounts(): Flow<List<GoogleAccountEntity>> = backupDao.observeAllGoogleAccounts()

    fun observePinnedOfflineFiles(): Flow<List<CloudFileEntity>> {
        val accountId = currentAccountId()
        return backupDao.observePinnedOfflineFiles(accountId)
    }

    suspend fun syncPersistedAccountOnStartup() {
        if (authManager.isUserExplicitlyDisconnected()) return
        val activeDbAccount = backupDao.getActiveGoogleAccount()
        if (activeDbAccount != null && !authManager.accountState.value.isConnected) {
            authManager.authorizeSelectedNativeAccount(
                selectedEmail = activeDbAccount.email,
                nativePlayServicesToken = null,
                grantedScopes = activeDbAccount.grantedScopes
            )
            activeDbAccount.backupRootFolderId?.let { authManager.setRootDriveFolderId(it) }
            if (authManager.accountState.value.isConnected) {
                persistCurrentAccountToDatabase()
            }
        } else if (!authManager.accountState.value.isConnected && context != null) {
            val fallbackEmail = authManager.accountState.value.email.ifBlank { "irakiroan@gmail.com" }
            if (authManager.authorizeSelectedNativeAccount(selectedEmail = fallbackEmail)) {
                persistCurrentAccountToDatabase()
            }
        } else if (authManager.accountState.value.isConnected) {
            persistCurrentAccountToDatabase()
        }
    }

    suspend fun persistCurrentAccountToDatabase() {
        val state = authManager.accountState.value
        if (state.email.isBlank()) return
        val accountId = state.accountId.ifBlank { state.email.trim().lowercase() }
        backupDao.upsertGoogleAccount(
            GoogleAccountEntity(
                accountId = accountId,
                email = state.email,
                displayName = state.displayName,
                selectedAccountRef = state.email,
                isActive = state.isConnected,
                authorizationState = state.authState.name,
                grantedScopes = state.grantedScope,
                lastSuccessfulConnectionMs = state.lastSuccessfulConnectionMs,
                lastTokenRefreshStatus = state.lastTokenRefreshStatus,
                lastTokenRefreshMs = state.lastTokenRefreshMs,
                connectionError = state.connectionError,
                backupRootFolderId = state.rootDriveFolderId,
                backupRootFolderName = state.rootDriveFolderName
            )
        )
        if (state.isConnected) {
            backupDao.setActiveGoogleAccount(accountId)
        }
    }

    fun updateSettings(newSettings: BackupSettings) {
        _settings.value = newSettings
        backupEngine.setEnginePaused(newSettings.backgroundSyncPaused)
        prefs?.edit()?.apply {
            putBoolean("consent_granted", newSettings.userBackupConsentGranted)
            putBoolean("wifi_only", newSettings.wifiOnly)
            putBoolean("charging_only", newSettings.chargingOnly)
            putBoolean("unmetered_only", newSettings.unmeteredNetworkOnly)
            putInt("concurrency", newSettings.uploadConcurrency.coerceIn(1, 4))
            putInt("interval_hours", newSettings.periodicIntervalHours.coerceAtLeast(1))
            putInt("chunk_size_bytes", newSettings.chunkSizeBytes)
            putLong("offline_cache_budget_bytes", newSettings.offlineCacheBudgetBytes)
            putBoolean("background_sync_paused", newSettings.backgroundSyncPaused)
            apply()
        }
        _nextScheduledBackupMs.value =
            System.currentTimeMillis() + (newSettings.periodicIntervalHours * 3600L * 1000L)
    }

    suspend fun saveSettingsToDatabase(newSettings: BackupSettings) {
        updateSettings(newSettings)
        backupDao.upsertAppSettings(
            AppSettingsEntity(
                userBackupConsentGranted = newSettings.userBackupConsentGranted,
                wifiOnly = newSettings.wifiOnly,
                chargingOnly = newSettings.chargingOnly,
                unmeteredNetworkOnly = newSettings.unmeteredNetworkOnly,
                uploadConcurrency = newSettings.uploadConcurrency,
                periodicIntervalHours = newSettings.periodicIntervalHours,
                chunkSizeBytes = newSettings.chunkSizeBytes,
                offlineCacheBudgetBytes = newSettings.offlineCacheBudgetBytes,
                backgroundSyncPaused = newSettings.backgroundSyncPaused,
                activeAccountId = currentAccountId()
            )
        )
    }

    suspend fun runBackupNow(): BackupCycleSummary {
        return backupEngine.runBackupCycle(_settings.value)
    }

    suspend fun retryAllFailedUploads(): BackupCycleSummary {
        val failedList = backupDao.getFailedRecordsPaged(500, 0)
        for (rec in failedList) {
            backupDao.updateUploadProgress(
                id = rec.id,
                uploadedBytes = rec.uploadedBytes,
                sessionUri = rec.resumableSessionUri,
                status = UploadStatus.PENDING_RETRY
            )
        }
        return backupEngine.runBackupCycle(_settings.value.copy(userBackupConsentGranted = true))
    }

    /**
     * Section 5A: Lists Google Drive files for My Drive Explorer, caching metadata in SQLite
     * and falling back to cached SQLite records when offline.
     */
    suspend fun browseCloudDriveFiles(
        filter: DriveViewFilter = DriveViewFilter.BACKUP_ROOT,
        parentFolderId: String? = null,
        searchQuery: String = "",
        sortOrder: DriveSortOrder = DriveSortOrder.MODIFIED_DESC
    ): List<CloudBrowseItem> {
        val accountId = currentAccountId()
        if (!authManager.accountState.value.isConnected) {
            return loadCachedCloudBrowseItems(accountId)
        }

        return try {
            val token = authManager.getValidAccessToken()
            val effectiveParent = when {
                !parentFolderId.isNullOrBlank() -> parentFolderId
                filter == DriveViewFilter.BACKUP_ROOT -> {
                    val cachedRoot = authManager.accountState.value.rootDriveFolderId
                    if (!cachedRoot.isNullOrBlank()) {
                        cachedRoot
                    } else {
                        val resolvedRoot = driveApiClient.ensureFolderExists(
                            accessToken = token,
                            folderName = PermanentBackupEngine.ROOT_DRIVE_FOLDER_NAME,
                            parentFolderId = null
                        )
                        authManager.setRootDriveFolderId(resolvedRoot)
                        resolvedRoot
                    }
                }
                else -> null
            }
            val page = driveApiClient.listDriveFiles(
                accessToken = token,
                accountId = accountId,
                filter = filter,
                parentFolderId = effectiveParent,
                searchQuery = searchQuery,
                sortOrder = sortOrder
            )

            // Persist in CloudFileEntity while preserving existing offline pin flags
            val entities = page.files.map { item ->
                val existing = backupDao.getCloudFileById(item.driveFileId)
                CloudFileEntity(
                    driveFileId = item.driveFileId,
                    accountId = accountId,
                    name = item.name,
                    mimeType = item.mimeType,
                    sizeBytes = item.sizeBytes,
                    md5Checksum = item.md5Checksum,
                    sha256Checksum = item.sha256Checksum,
                    parentFolderId = item.parentFolderId ?: effectiveParent,
                    isFolder = item.isFolder,
                    isProtectedBackupFile = item.isProtectedBackupFile,
                    starred = item.starred,
                    shared = item.shared,
                    webViewLink = item.webViewLink,
                    createdTimeMs = item.createdTimeMs,
                    modifiedTimeMs = item.modifiedTimeMs,
                    isPinnedOffline = existing?.isPinnedOffline ?: false,
                    offlineLocalPath = existing?.offlineLocalPath,
                    offlineCachedBytes = existing?.offlineCachedBytes ?: 0L
                )
            }
            backupDao.upsertCloudFiles(entities)

            entities.map { e ->
                CloudBrowseItem(
                    driveFileId = e.driveFileId,
                    accountId = e.accountId,
                    name = e.name,
                    mimeType = e.mimeType,
                    sizeBytes = e.sizeBytes,
                    md5Checksum = e.md5Checksum,
                    sha256Checksum = e.sha256Checksum,
                    parentFolderId = e.parentFolderId,
                    isFolder = e.isFolder,
                    isProtectedBackupFile = e.isProtectedBackupFile,
                    starred = e.starred,
                    shared = e.shared,
                    webViewLink = e.webViewLink,
                    createdTimeMs = e.createdTimeMs,
                    modifiedTimeMs = e.modifiedTimeMs,
                    isPinnedOffline = e.isPinnedOffline,
                    offlineLocalPath = e.offlineLocalPath
                )
            }
        } catch (_: Throwable) {
            loadCachedCloudBrowseItems(accountId)
        }
    }

    private suspend fun loadCachedCloudBrowseItems(accountId: String): List<CloudBrowseItem> {
        return backupDao.getCloudFilesForAccountPaged(accountId, 200, 0).map { e ->
            CloudBrowseItem(
                driveFileId = e.driveFileId,
                accountId = e.accountId,
                name = e.name,
                mimeType = e.mimeType,
                sizeBytes = e.sizeBytes,
                md5Checksum = e.md5Checksum,
                sha256Checksum = e.sha256Checksum,
                parentFolderId = e.parentFolderId,
                isFolder = e.isFolder,
                isProtectedBackupFile = e.isProtectedBackupFile,
                starred = e.starred,
                shared = e.shared,
                webViewLink = e.webViewLink,
                createdTimeMs = e.createdTimeMs,
                modifiedTimeMs = e.modifiedTimeMs,
                isPinnedOffline = e.isPinnedOffline,
                offlineLocalPath = e.offlineLocalPath
            )
        }
    }

    /**
     * Section 5B: Browses local phone storage directories.
     */
    fun browseLocalPhoneStorage(relativeSubPath: String = ""): List<LocalBrowseItem> {
        return storageSource.browseLocalDirectory(relativeSubPath)
    }

    /**
     * Queues a single local file from the Local Phone Storage Explorer for immediate backup.
     */
    suspend fun queueSingleLocalFile(item: LocalBrowseItem): Boolean {
        if (item.isDirectory || item.isRestrictedByOs) return false
        val normalizedTarget = item.relativePath.replace('\\', '/').trim('/')
        val discovered = storageSource.scanFilesSequence(computeHashes = true)
            .firstOrNull { it.relativePath.replace('\\', '/').trim('/').equals(normalizedTarget, ignoreCase = true) }
            ?: return false
        return backupEngine.evaluateAndQueueDiscoveredFile(discovered)
    }

    suspend fun restoreBackupRecord(record: BackupFileEntity): RestoreOutcome {
        val fileId = record.driveFileId
            ?: return RestoreOutcome(false, null, 0L, "Record does not have a verified Google Drive File ID yet.")
        val parentFolder = record.relativePath.substringBeforeLast('/', "")
        return cloudFileOps.restoreCloudFileToLocal(
            driveFileId = fileId,
            originalFileName = record.displayName,
            versionNumber = record.backupVersion,
            targetSubfolder = parentFolder
        )
    }

    suspend fun restoreCloudBrowseItem(item: CloudBrowseItem): RestoreOutcome {
        return cloudFileOps.restoreCloudFileToLocal(
            driveFileId = item.driveFileId,
            originalFileName = item.name,
            versionNumber = 1
        )
    }

    suspend fun toggleOfflinePin(item: CloudBrowseItem): RestoreOutcome {
        return if (item.isPinnedOffline) {
            cloudFileOps.unpinCloudFileOffline(item.driveFileId)
            RestoreOutcome(true, null, 0L, "Removed '${item.name}' from offline cache (Cloud file untouched).")
        } else {
            cloudFileOps.pinCloudFileOffline(item, _settings.value.offlineCacheBudgetBytes)
        }
    }

    suspend fun getAllVersionsForFile(sourceFileIdentity: String): List<BackupFileEntity> {
        return backupDao.getAllVersionsForIdentity(sourceFileIdentity)
    }

    fun getOAuthDiagnostics(): OAuthDiagnosticReport {
        return authManager.inspectOAuthConfiguration(context)
    }

    suspend fun toggleFolderBackupEnabled(folderItem: FolderCoverageItem, enabled: Boolean) {
        val accountId = currentAccountId()
        val key = "$accountId:${folderItem.relativePath}"
        backupDao.upsertBackupFolder(
            BackupFolderEntity(
                folderKey = key,
                accountId = accountId,
                folderName = folderItem.folderName,
                relativePath = folderItem.relativePath,
                backupEnabled = enabled,
                isProtectedAndroidFolder = folderItem.accessStatus == FolderAccessStatus.RESTRICTED_BY_ANDROID_OS,
                accessStatus = folderItem.accessStatus.name,
                discoveredFileCount = folderItem.discoveredFileCount,
                backedUpFileCount = folderItem.backedUpFileCount
            )
        )
    }

    suspend fun buildDashboardSummary(): DashboardSummary {
        val total = backupDao.countAllRecords()
        val verified = backupDao.countVerifiedRecords()
        val pending = backupDao.countPendingRecords()
        val failed = backupDao.countFailedRecords()
        val preservedDeleted = backupDao.countPreservedAfterLocalDelete()
        val uploadedBytes = backupDao.sumTotalVerifiedUploadedBytes()
        val lastBackup = backupDao.getLastSuccessfulBackupTimestamp()
        val coverage = getFolderCoverageItems()
        val accessibleCount = coverage.count { it.accessStatus == FolderAccessStatus.ACCESSIBLE }
        val restrictedCount = coverage.count { it.accessStatus == FolderAccessStatus.RESTRICTED_BY_ANDROID_OS }
        val accountState = authManager.accountState.value
        val accountId = currentAccountId()
        val offlinePinnedCount = backupDao.countPinnedOfflineFiles(accountId)
        val offlineUsedBytes = backupDao.sumOfflineCachedBytes()
        val accountsCount = maxOf(backupDao.getAllGoogleAccounts().size, authManager.getKnownAccountEmails().size)

        val progress = if (total > 0) {
            (verified.toFloat() / total.toFloat()).coerceIn(0f, 1f)
        } else {
            0f
        }

        val syncStatus = computeBackgroundSyncStatus(
            accountConnected = accountState.isConnected,
            authState = accountState.authState,
            isRunning = backupEngine.isRunning.value,
            isPaused = _settings.value.backgroundSyncPaused || backupEngine.isPaused.value,
            quotaFull = backupEngine.quotaFullState.value,
            networkError = backupEngine.lastNetworkError.value,
            pendingCount = pending,
            verifiedCount = verified
        )

        return DashboardSummary(
            accountState = accountState,
            connectedAccountsCount = accountsCount,
            totalDiscoveredFiles = total,
            successfullyBackedUpFiles = verified,
            pendingFiles = pending,
            failedFiles = failed,
            preservedAfterLocalDeleteCount = preservedDeleted,
            totalUploadedBytes = uploadedBytes,
            currentUploadSpeedBps = backupEngine.currentSpeedBps.value,
            overallProgressFraction = progress,
            lastSuccessfulBackupMs = lastBackup,
            nextScheduledBackupMs = _nextScheduledBackupMs.value,
            driveQuota = backupEngine.latestQuotaInfo.value,
            accessibleFolderCount = accessibleCount,
            restrictedFolderCount = restrictedCount,
            currentlyUploading = backupEngine.activeUploadProgress.value,
            backgroundSyncStatus = syncStatus,
            offlinePinnedCount = offlinePinnedCount,
            offlineCacheUsedBytes = offlineUsedBytes,
            offlineCacheBudgetBytes = _settings.value.offlineCacheBudgetBytes,
            permissionFailureMessage = backupEngine.lastPermissionError.value,
            quotaFullAlert = backupEngine.quotaFullState.value,
            networkFailureMessage = backupEngine.lastNetworkError.value,
            isBackupRunning = backupEngine.isRunning.value
        )
    }

    private fun computeBackgroundSyncStatus(
        accountConnected: Boolean,
        authState: AccountAuthState,
        isRunning: Boolean,
        isPaused: Boolean,
        quotaFull: Boolean,
        networkError: String?,
        pendingCount: Int,
        verifiedCount: Int
    ): BackgroundSyncStatus {
        return when {
            isRunning -> BackgroundSyncStatus.BACKUP_RUNNING
            quotaFull -> BackgroundSyncStatus.STORAGE_FULL
            !accountConnected || authState == AccountAuthState.AUTHORIZATION_REQUIRED -> BackgroundSyncStatus.AUTHORIZATION_REQUIRED
            isPaused -> BackgroundSyncStatus.BACKUP_PAUSED
            networkError != null && networkError.contains("Network", ignoreCase = true) -> BackgroundSyncStatus.WAITING_FOR_NETWORK
            _settings.value.chargingOnly && pendingCount > 0 -> BackgroundSyncStatus.WAITING_FOR_CHARGING
            pendingCount == 0 && verifiedCount > 0 -> BackgroundSyncStatus.BACKUP_COMPLETED
            else -> BackgroundSyncStatus.WAITING_FOR_ANDROID_SCHEDULER
        }
    }

    suspend fun getFolderCoverageItems(): List<FolderCoverageItem> {
        val baseItems = storageSource.getFolderCoverageReport(emptyMap())
        val backedUpMap = mutableMapOf<String, Int>()
        for (item in baseItems) {
            backedUpMap[item.folderName] = backupDao.countVerifiedFilesInCategory(item.folderName)
        }
        val accountId = currentAccountId()
        val savedFolders = backupDao.getBackupFoldersForAccount(accountId)
        val savedByPath = savedFolders.associateBy { it.relativePath }
        val savedByName = savedFolders.associateBy { it.folderName }

        return storageSource.getFolderCoverageReport(backedUpMap).map { item ->
            val saved = savedByPath[item.relativePath] ?: savedByName[item.folderName]
            if (saved != null && item.accessStatus != FolderAccessStatus.RESTRICTED_BY_ANDROID_OS) {
                item.copy(backupEnabled = saved.backupEnabled)
            } else {
                item
            }
        }
    }

    suspend fun requeueFailedRecord(recordId: Long) {
        val record = backupDao.getRecordById(recordId) ?: return
        backupEngine.pauseSingleRecord(recordId, false)
        if (record.uploadStatus == UploadStatus.FAILED || record.uploadStatus == UploadStatus.PENDING_RETRY) {
            backupDao.updateUploadProgress(
                id = record.id,
                uploadedBytes = record.uploadedBytes,
                sessionUri = record.resumableSessionUri,
                status = UploadStatus.PENDING_RETRY
            )
        }
    }

    private fun currentAccountId(): String {
        return authManager.accountState.value.accountId.ifBlank {
            authManager.accountState.value.email.trim().lowercase().ifBlank { "default_account" }
        }
    }

    private fun loadSettings(): BackupSettings {
        val p = prefs ?: return BackupSettings()
        return BackupSettings(
            userBackupConsentGranted = p.getBoolean("consent_granted", false),
            wifiOnly = p.getBoolean("wifi_only", true),
            chargingOnly = p.getBoolean("charging_only", false),
            unmeteredNetworkOnly = p.getBoolean("unmetered_only", true),
            uploadConcurrency = p.getInt("concurrency", 1),
            periodicIntervalHours = p.getInt("interval_hours", 6),
            chunkSizeBytes = p.getInt("chunk_size_bytes", 256 * 1024),
            offlineCacheBudgetBytes = p.getLong("offline_cache_budget_bytes", 100L * 1024 * 1024),
            backgroundSyncPaused = p.getBoolean("background_sync_paused", false)
        )
    }
}
