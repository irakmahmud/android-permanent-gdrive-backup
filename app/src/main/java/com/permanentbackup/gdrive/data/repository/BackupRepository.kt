package com.permanentbackup.gdrive.data.repository

import android.content.Context
import android.content.SharedPreferences
import com.permanentbackup.gdrive.data.local.AppSettingsEntity
import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.BackupFolderEntity
import com.permanentbackup.gdrive.data.local.BackupVersionEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.FailedOperationEntity
import com.permanentbackup.gdrive.data.local.GoogleAccountEntity
import com.permanentbackup.gdrive.data.local.StoragePermissionEntity
import com.permanentbackup.gdrive.data.local.SyncEventEntity
import com.permanentbackup.gdrive.data.local.SyncJobEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.engine.AlwaysSatisfiedDeviceConditions
import com.permanentbackup.gdrive.domain.engine.AndroidDeviceConditionsProvider
import com.permanentbackup.gdrive.domain.engine.BackupCycleSummary
import com.permanentbackup.gdrive.domain.engine.CloudFileOperationsManager
import com.permanentbackup.gdrive.domain.engine.PermanentBackupEngine
import com.permanentbackup.gdrive.domain.engine.SourceFileReadOnlyGuard
import com.permanentbackup.gdrive.domain.engine.StorageFileSource
import com.permanentbackup.gdrive.domain.model.AccountAuthState
import com.permanentbackup.gdrive.domain.model.BackgroundSyncStatus
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.DiscoveredSourceFile
import com.permanentbackup.gdrive.domain.model.DriveSortOrder
import com.permanentbackup.gdrive.domain.model.DriveViewFilter
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.LocalBrowseItem
import com.permanentbackup.gdrive.domain.model.OAuthDiagnosticReport
import com.permanentbackup.gdrive.domain.model.RestoreOutcome
import com.permanentbackup.gdrive.domain.model.UploadStatus
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.io.FileOutputStream

class BackupRepository(
    private val context: Context? = null,
    val backupDao: BackupDao,
    val storageSource: StorageFileSource,
    val driveApiClient: DriveApiClient,
    val authManager: GoogleAuthManager,
    customSettingsPrefs: SharedPreferences? = null
) {
    companion object {
        const val SHARE_STAGING_DIR_NAME = "share_staging"
        /** Larger shares stream directly from the content URI instead of being staged (storage protection). */
        const val MAX_SHARE_STAGING_BYTES = 512L * 1024L * 1024L
        private const val PREF_MIRRORED_FOLDERS = "mirrored_cloud_folders_v1"
    }

    private val prefs: SharedPreferences? = customSettingsPrefs
        ?: context?.applicationContext?.getSharedPreferences("permanent_backup_settings", Context.MODE_PRIVATE)

    val backupEngine = PermanentBackupEngine(
        backupDao = backupDao,
        storageSource = storageSource,
        driveApiClient = driveApiClient,
        authManager = authManager,
        deviceConditions = context?.let { AndroidDeviceConditionsProvider(it) } ?: AlwaysSatisfiedDeviceConditions
    )

    val cloudFileOps = CloudFileOperationsManager(
        context = context,
        backupDao = backupDao,
        driveApiClient = driveApiClient,
        authManager = authManager
    )

    private val _settings = MutableStateFlow(loadSettings())
    val settings: StateFlow<BackupSettings> = _settings.asStateFlow()

    private val _cloudNextPageToken = MutableStateFlow<String?>(null)
    val cloudNextPageToken: StateFlow<String?> = _cloudNextPageToken.asStateFlow()

    private val _nextScheduledBackupMs = MutableStateFlow<Long?>(
        System.currentTimeMillis() + (6L * 3600L * 1000L)
    )
    val nextScheduledBackupMs: StateFlow<Long?> = _nextScheduledBackupMs.asStateFlow()

    /** Active account ID stream; every UI list re-subscribes to the right account on account switch. */
    private val activeAccountIdFlow: Flow<String> = authManager.accountState
        .map { state -> state.accountId.ifBlank { state.email.trim().lowercase().ifBlank { "default_account" } } }
        .distinctUntilChanged()

    private val mirrorSyncMutex = Mutex()

    init {
        // Automatically refresh any pinned offline file when Google Drive Changes API detects a remote edit
        // (also auto-pins new files that appear inside a "Keep Folder Offline" folder)
        backupEngine.onPinnedCloudFileModified = { modifiedItem ->
            cloudFileOps.pinCloudFileOffline(modifiedItem, _settings.value.offlineCacheBudgetBytes)
        }
        // Keep one-way Cloud -> Phone Mirror folders up to date whenever Drive reports changes
        backupEngine.onCloudChangesDetected = { _ ->
            syncRegisteredMirrorFolders()
        }
        // Remove the app-private share staging copy once its bytes are VERIFIED on Google Drive
        backupEngine.onRecordVerified = { record ->
            deleteShareStagingCopyIfAny(record.sourceUri)
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeActiveQueue(): Flow<List<BackupFileEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeActiveQueueForAccount(it) }
            .catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeVerifiedHistory(): Flow<List<BackupFileEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeVerifiedHistoryForAccount(it) }
            .catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeFailedUploads(): Flow<List<BackupFileEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeFailedRecordsForAccount(it) }
            .catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeRecentRecords(): Flow<List<BackupFileEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeRecentRecordsForAccount(it) }
            .catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeSyncJobs(): Flow<List<SyncJobEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeSyncJobsForAccount(it) }
            .catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeRecentSyncEvents(): Flow<List<SyncEventEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeRecentSyncEventsForAccount(it) }
            .catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observeUnresolvedFailures(): Flow<List<FailedOperationEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observeUnresolvedFailuresForAccount(it) }
            .catch { emit(emptyList()) }

    fun observeGoogleAccounts(): Flow<List<GoogleAccountEntity>> =
        backupDao.observeAllGoogleAccounts().catch { emit(emptyList()) }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun observePinnedOfflineFiles(): Flow<List<CloudFileEntity>> =
        activeAccountIdFlow.flatMapLatest { backupDao.observePinnedOfflineFiles(it) }
            .catch { emit(emptyList()) }

    suspend fun syncPersistedAccountOnStartup() {
        try {
            // 1. Restore persisted AppSettingsEntity from Room if present; always ensure auto-backup consent is active
            val dbSettings = backupDao.getAppSettingsEntity()
            if (dbSettings != null) {
                val restored = BackupSettings(
                    userBackupConsentGranted = true,
                    wifiOnly = dbSettings.wifiOnly,
                    chargingOnly = dbSettings.chargingOnly,
                    unmeteredNetworkOnly = dbSettings.unmeteredNetworkOnly,
                    uploadConcurrency = dbSettings.uploadConcurrency.coerceIn(1, 4),
                    periodicIntervalHours = dbSettings.periodicIntervalHours.coerceAtLeast(1),
                    chunkSizeBytes = dbSettings.chunkSizeBytes,
                    offlineCacheBudgetBytes = dbSettings.offlineCacheBudgetBytes,
                    backgroundSyncPaused = dbSettings.backgroundSyncPaused,
                    maxUploadSpeedKbps = dbSettings.maxUploadSpeedKbps.coerceAtLeast(0)
                )
                _settings.value = restored
                backupEngine.setEnginePaused(restored.backgroundSyncPaused)
                if (!dbSettings.userBackupConsentGranted) {
                    saveSettingsToDatabase(restored)
                }
            } else {
                val defaultAutoSettings = _settings.value.copy(userBackupConsentGranted = true)
                saveSettingsToDatabase(defaultAutoSettings)
            }

            // 2. Restore persisted SAF Tree URIs into storageSource so custom SAF folders are scanned
            val savedPerms = backupDao.getAllStoragePermissions()
            for (perm in savedPerms) {
                if (perm.granted && !perm.persistedTreeUri.isNullOrBlank()) {
                    storageSource.registerPersistedSafTreeUri(perm.persistedTreeUri)
                }
            }

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
        } catch (_: Throwable) {
            // Never crash startup if local DB, Keystore, or network is temporarily unavailable
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

    fun setBackupPaused(paused: Boolean) {
        updateSettings(_settings.value.copy(backgroundSyncPaused = paused))
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
            putInt("max_upload_speed_kbps", newSettings.maxUploadSpeedKbps.coerceAtLeast(0))
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
                uploadConcurrency = newSettings.uploadConcurrency.coerceIn(1, 4),
                periodicIntervalHours = newSettings.periodicIntervalHours.coerceAtLeast(1),
                chunkSizeBytes = newSettings.chunkSizeBytes,
                offlineCacheBudgetBytes = newSettings.offlineCacheBudgetBytes,
                backgroundSyncPaused = newSettings.backgroundSyncPaused,
                maxUploadSpeedKbps = newSettings.maxUploadSpeedKbps.coerceAtLeast(0),
                activeAccountId = currentAccountId()
            )
        )
    }

    /**
     * Persists a user-granted SAF directory tree URI in Room and registers it with the scanner.
     */
    suspend fun registerPersistedSafFolder(treeUriString: String, label: String = "SAF_Custom") {
        if (treeUriString.isBlank()) return
        storageSource.registerPersistedSafTreeUri(treeUriString)
        val permKey = "SAF_TREE:${treeUriString.hashCode()}"
        backupDao.upsertStoragePermission(
            StoragePermissionEntity(
                permissionKey = permKey,
                granted = true,
                scopeType = "SAF_TREE",
                persistedTreeUri = treeUriString,
                androidSdkLevel = android.os.Build.VERSION.SDK_INT,
                statusExplanation = "Persisted SAF Directory Tree ($label)"
            )
        )
        val accountId = currentAccountId()
        backupDao.upsertBackupFolder(
            BackupFolderEntity(
                folderKey = "$accountId:SAF/$label",
                accountId = accountId,
                folderName = label,
                relativePath = "SAF/$label",
                backupEnabled = true,
                isProtectedAndroidFolder = false,
                accessStatus = FolderAccessStatus.ACCESSIBLE.name
            )
        )
    }

    /**
     * Adds or toggles per-folder Auto-Backup for any subfolder browsed in Phone Storage Explorer.
     */
    suspend fun toggleCustomLocalFolderAutoBackup(localItem: LocalBrowseItem, enabled: Boolean) {
        if (!localItem.isDirectory) return
        val accountId = currentAccountId()
        val relPath = localItem.relativePath.replace('\\', '/').trim('/')
        val key = "$accountId:$relPath"
        backupDao.upsertBackupFolder(
            BackupFolderEntity(
                folderKey = key,
                accountId = accountId,
                folderName = localItem.name,
                relativePath = relPath,
                backupEnabled = enabled,
                isProtectedAndroidFolder = localItem.isRestrictedByOs,
                accessStatus = if (localItem.isRestrictedByOs) {
                    FolderAccessStatus.RESTRICTED_BY_ANDROID_OS.name
                } else {
                    FolderAccessStatus.ACCESSIBLE.name
                }
            )
        )
    }

    /**
     * Full scan + upload cycle.
     * @param respectRetryBackoff true for automatic/background triggers (WorkManager) so failed files wait for
     *   their exponential backoff window; manual "Backup Now" retries everything immediately.
     */
    suspend fun runBackupNow(respectRetryBackoff: Boolean = false): BackupCycleSummary {
        return backupEngine.runBackupCycle(_settings.value, respectRetryBackoff)
    }

    /**
     * Uploads already-queued records only (no full storage rescan). Used after share-sheet / "Upload File",
     * single-file "Backup File", per-file Retry/Resume so they start instantly even on huge phones.
     */
    suspend fun runUploadQueueNow(): BackupCycleSummary {
        return backupEngine.runUploadQueueOnly(_settings.value)
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
            backupDao.clearSyncJobRetryBackoff(rec.id)
        }
        return backupEngine.runUploadQueueOnly(_settings.value.copy(userBackupConsentGranted = true))
    }

    /**
     * Section 5A: Lists Google Drive files for My Drive Explorer with pagination (`pageToken` / `cloudNextPageToken`),
     * caching metadata in SQLite and falling back to filter/folder-aware cached SQLite records when offline.
     */
    suspend fun browseCloudDriveFiles(
        filter: DriveViewFilter = DriveViewFilter.BACKUP_ROOT,
        parentFolderId: String? = null,
        searchQuery: String = "",
        sortOrder: DriveSortOrder = DriveSortOrder.MODIFIED_DESC,
        pageToken: String? = null
    ): List<CloudBrowseItem> {
        val accountId = currentAccountId()
        if (!authManager.accountState.value.isConnected) {
            _cloudNextPageToken.value = null
            return loadCachedCloudBrowseItems(accountId, filter, parentFolderId, searchQuery)
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
                sortOrder = sortOrder,
                pageToken = pageToken
            )
            _cloudNextPageToken.value = page.nextPageToken

            // Persist in CloudFileEntity while preserving existing offline pin flags
            val entities = ArrayList<CloudFileEntity>(page.files.size)
            for (item in page.files) {
                val existing = backupDao.getCloudFileById(item.driveFileId)
                entities.add(
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
                )
            }
            backupDao.upsertCloudFiles(entities)

            entities.map { e -> e.toCloudBrowseItem() }
        } catch (_: Throwable) {
            _cloudNextPageToken.value = null
            loadCachedCloudBrowseItems(accountId, filter, parentFolderId, searchQuery)
        }
    }

    private suspend fun loadCachedCloudBrowseItems(
        accountId: String,
        filter: DriveViewFilter = DriveViewFilter.BACKUP_ROOT,
        parentFolderId: String? = null,
        searchQuery: String = ""
    ): List<CloudBrowseItem> {
        val rawEntities = when {
            searchQuery.isNotBlank() -> backupDao.searchCloudFiles(accountId, searchQuery.trim())
            !parentFolderId.isNullOrBlank() -> backupDao.getCloudFilesByParent(parentFolderId)
            else -> backupDao.getCloudFilesForAccountPaged(accountId, 200, 0)
        }
        val filtered = rawEntities.filter { e ->
            when {
                searchQuery.isNotBlank() || !parentFolderId.isNullOrBlank() -> true
                filter == DriveViewFilter.STARRED -> e.starred
                filter == DriveViewFilter.SHARED_WITH_ME -> e.shared
                else -> true
            }
        }
        return filtered.map { it.toCloudBrowseItem() }
    }

    private fun CloudFileEntity.toCloudBrowseItem(): CloudBrowseItem = CloudBrowseItem(
        driveFileId = driveFileId,
        accountId = accountId,
        name = name,
        mimeType = mimeType,
        sizeBytes = sizeBytes,
        md5Checksum = md5Checksum,
        sha256Checksum = sha256Checksum,
        parentFolderId = parentFolderId,
        isFolder = isFolder,
        isProtectedBackupFile = isProtectedBackupFile,
        starred = starred,
        shared = shared,
        webViewLink = webViewLink,
        createdTimeMs = createdTimeMs,
        modifiedTimeMs = modifiedTimeMs,
        isPinnedOffline = isPinnedOffline,
        offlineLocalPath = offlineLocalPath
    )

    /**
     * Creates a new subfolder on Google Drive (for My Drive Explorer) and indexes it locally.
     */
    suspend fun createCloudFolder(folderName: String, parentFolderId: String? = null): String? {
        if (folderName.isBlank() || !authManager.accountState.value.isConnected) return null
        return try {
            val token = authManager.getValidAccessToken()
            val effectiveParent = parentFolderId ?: authManager.accountState.value.rootDriveFolderId
            val createdId = driveApiClient.ensureFolderExists(
                accessToken = token,
                folderName = folderName.trim(),
                parentFolderId = effectiveParent
            )
            val now = System.currentTimeMillis()
            backupDao.upsertCloudFile(
                CloudFileEntity(
                    driveFileId = createdId,
                    accountId = currentAccountId(),
                    name = folderName.trim(),
                    mimeType = DriveApiClient.FOLDER_MIME_TYPE,
                    sizeBytes = 0L,
                    parentFolderId = effectiveParent,
                    isFolder = true,
                    isProtectedBackupFile = true,
                    createdTimeMs = now,
                    modifiedTimeMs = now
                )
            )
            createdId
        } catch (_: Throwable) {
            null
        }
    }

    /**
     * Queues a shared or picked URI (`ACTION_SEND` / `ACTION_SEND_MULTIPLE` or "Upload File" button)
     * directly into the permanent backup queue.
     *
     * - [targetDriveFolderId]: when the user is inside a My Drive folder, the file is uploaded straight into that
     *   exact Drive folder (identity `shared:drive:<folderId>/<name>`) instead of a mirrored subfolder.
     * - Temporary share-sheet grants (`content://` without a persistable permission) expire when the sending app
     *   dies, so the bytes are first staged into app-private storage; the staging copy is removed automatically
     *   after the Drive upload is VERIFIED.
     */
    suspend fun uploadDirectUriToDrive(
        uriString: String,
        displayName: String,
        mimeType: String,
        sizeBytes: Long,
        targetSubfolder: String = "Shared_To_Drive",
        targetDriveFolderId: String? = null,
        stageTemporaryGrant: Boolean = false
    ): Boolean {
        if (uriString.isBlank()) return false
        val safeDisplay = cloudFileOps.sanitizeSafeLocalFileName(
            displayName.ifBlank { "shared_file_${System.currentTimeMillis()}" }
        )
        val cleanFolder = targetSubfolder.replace('\\', '/').trim('/').ifBlank { "Shared_To_Drive" }
        val relPath = "$cleanFolder/$safeDisplay"
        val explicitFolder = targetDriveFolderId?.trim()?.takeIf { it.isNotBlank() && !it.contains('/') }
        val identity = if (explicitFolder != null) {
            "${PermanentBackupEngine.SHARED_DRIVE_TARGET_PREFIX}$explicitFolder/$safeDisplay"
        } else {
            "shared:$relPath"
        }
        val now = System.currentTimeMillis()

        val effectiveSourceUri = if (stageTemporaryGrant) {
            stageSharedContentLocally(uriString, safeDisplay, sizeBytes) ?: uriString
        } else {
            uriString
        }

        val hashes = try {
            if (sizeBytes in 0L..(64L * 1024L * 1024L)) {
                SourceFileReadOnlyGuard.computeStreamingHashes {
                    storageSource.openInputStream(effectiveSourceUri)
                }
            } else null
        } catch (_: Throwable) {
            null
        }

        val stagedLength = if (effectiveSourceUri != uriString) File(effectiveSourceUri).length() else 0L
        val resolvedSize = when {
            sizeBytes > 0L -> sizeBytes
            stagedLength > 0L -> stagedLength
            else -> hashes?.totalBytesRead ?: 0L
        }
        val discovered = DiscoveredSourceFile(
            sourceUri = effectiveSourceUri,
            sourceFileIdentity = identity,
            relativePath = relPath,
            displayName = safeDisplay,
            folderCategory = cleanFolder,
            fileSize = resolvedSize,
            lastModifiedMs = now,
            mimeType = mimeType.ifBlank { "application/octet-stream" },
            sha256Hash = hashes?.sha256Hex,
            md5Hash = hashes?.md5Hex
        )
        val queued = backupEngine.evaluateAndQueueDiscoveredFile(discovered)
        if (!queued) {
            // Already backed up & unchanged -> the staging copy is not needed
            deleteShareStagingCopyIfAny(effectiveSourceUri)
        }
        return queued
    }

    /** App-private directory holding share-sheet bytes until their Drive upload is verified. */
    fun shareStagingDirectory(): File? {
        val base = context?.filesDir ?: return null
        return File(base, SHARE_STAGING_DIR_NAME)
    }

    private fun stageSharedContentLocally(uriString: String, safeDisplay: String, sizeBytes: Long): String? {
        if (!uriString.startsWith("content://")) return null
        if (sizeBytes > MAX_SHARE_STAGING_BYTES) return null
        val stagingDir = shareStagingDirectory() ?: return null
        val stagedFile = File(stagingDir, "${System.currentTimeMillis()}_$safeDisplay")
        return try {
            if (!stagingDir.exists()) stagingDir.mkdirs()
            if (sizeBytes > 0L && stagingDir.usableSpace < sizeBytes * 2) return null
            var copied = 0L
            storageSource.openInputStream(uriString).use { input ->
                FileOutputStream(stagedFile).use { output ->
                    val buffer = ByteArray(64 * 1024)
                    while (true) {
                        val read = input.read(buffer)
                        if (read == -1) break
                        output.write(buffer, 0, read)
                        copied += read
                        if (copied > MAX_SHARE_STAGING_BYTES) {
                            throw java.io.IOException("Shared file exceeds staging limit")
                        }
                    }
                }
            }
            stagedFile.absolutePath
        } catch (_: Throwable) {
            // Only our own half-written app-private staging file is removed
            if (stagedFile.exists()) stagedFile.delete()
            null
        }
    }

    /**
     * Deletes ONLY the app's own private staging copy (never user files, never Google Drive data).
     */
    suspend fun deleteShareStagingCopyIfAny(sourceUri: String) {
        val stagingDir = shareStagingDirectory() ?: return
        if (sourceUri.startsWith("content://")) return
        try {
            val candidate = File(sourceUri)
            val inStaging = candidate.canonicalPath.startsWith(stagingDir.canonicalPath + File.separator)
            if (!inStaging || !candidate.exists()) return
            // Keep the bytes if another not-yet-verified record still points at this staged file
            val stillNeeded = backupDao.getPendingRecordsForAccountPaged(currentAccountId(), 500, 0)
                .any { it.sourceUri == sourceUri }
            if (!stillNeeded) candidate.delete()
        } catch (_: Throwable) {
        }
    }

    // ------------------------------------------------------------------------
    // One-way Cloud -> Phone Mirror registry (kept in sync via Drive Changes API)
    // ------------------------------------------------------------------------

    private fun mirrorRegistryEntries(): Set<String> =
        prefs?.getStringSet(PREF_MIRRORED_FOLDERS, emptySet())?.toSet() ?: emptySet()

    private fun registerMirrorFolder(item: CloudBrowseItem) {
        val p = prefs ?: return
        val accountId = item.accountId.ifBlank { currentAccountId() }
        val entry = "$accountId|${item.driveFileId}|${item.name}"
        val updated = mirrorRegistryEntries()
            .filterNot { it.split('|').getOrNull(1) == item.driveFileId }
            .toMutableSet()
            .apply { add(entry) }
        p.edit().putStringSet(PREF_MIRRORED_FOLDERS, updated).apply()
    }

    /** Mirror folders registered for the active account (driveFileId to folder name). */
    fun getRegisteredMirrorFolders(): List<Pair<String, String>> {
        val accountId = currentAccountId()
        return mirrorRegistryEntries().mapNotNull { raw ->
            val parts = raw.split('|', limit = 3)
            if (parts.size == 3 && parts[0] == accountId) parts[1] to parts[2] else null
        }
    }

    /** Stops future automatic mirror refreshes; already-mirrored local files are left untouched. */
    fun unregisterMirrorFolder(driveFolderId: String) {
        val p = prefs ?: return
        val updated = mirrorRegistryEntries().filterNot { it.split('|').getOrNull(1) == driveFolderId }.toSet()
        p.edit().putStringSet(PREF_MIRRORED_FOLDERS, updated).apply()
    }

    /**
     * Incrementally refreshes every registered Mirror folder: downloads only new or changed Drive files
     * (changed files are saved as a new non-overwriting local copy). Never touches Google Drive.
     */
    suspend fun syncRegisteredMirrorFolders(): Int {
        if (!authManager.accountState.value.isConnected) return 0
        val folders = getRegisteredMirrorFolders()
        if (folders.isEmpty()) return 0
        if (mirrorSyncMutex.isLocked) return 0
        return mirrorSyncMutex.withLock {
            var downloaded = 0
            val accountId = currentAccountId()
            for ((folderId, folderName) in folders) {
                val outcome = cloudFileOps.syncMirrorFolderIncremental(
                    CloudBrowseItem(
                        driveFileId = folderId,
                        accountId = accountId,
                        name = folderName,
                        mimeType = DriveApiClient.FOLDER_MIME_TYPE,
                        sizeBytes = 0L,
                        isFolder = true
                    )
                )
                if (outcome.success) downloaded += outcome.filesDownloaded
            }
            downloaded
        }
    }

    /**
     * Section 5B: Browses local phone storage directories while overlaying saved per-folder auto-backup toggles.
     */
    suspend fun browseLocalPhoneStorage(relativeSubPath: String = ""): List<LocalBrowseItem> {
        val rawItems = storageSource.browseLocalDirectory(relativeSubPath)
        val accountId = currentAccountId()
        val savedByPath = backupDao.getBackupFoldersForAccount(accountId)
            .associateBy { it.relativePath.replace('\\', '/').trim('/') }
        return rawItems.map { item ->
            val normRel = item.relativePath.replace('\\', '/').trim('/')
            val saved = savedByPath[normRel]
            if (item.isDirectory && !item.isRestrictedByOs && saved != null) {
                item.copy(backupEnabled = saved.backupEnabled)
            } else {
                item
            }
        }
    }

    /**
     * Queues a single local file from the Local Phone Storage Explorer for immediate backup.
     */
    suspend fun queueSingleLocalFile(item: LocalBrowseItem): Boolean {
        if (item.isDirectory || item.isRestrictedByOs) return false
        val normalizedTarget = item.relativePath.replace('\\', '/').trim('/')
        val discovered = storageSource.scanFilesSequence(computeHashes = false)
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
            targetSubfolder = parentFolder,
            mimeType = record.mimeType
        )
    }

    suspend fun restoreBackupVersionEntity(version: BackupVersionEntity): RestoreOutcome {
        if (version.driveFileId.isBlank()) {
            return RestoreOutcome(false, null, 0L, "Version does not have a verified Google Drive File ID.")
        }
        val parentFolder = version.relativePath.substringBeforeLast('/', "")
        val originalName = version.relativePath.substringAfterLast('/').ifBlank { version.driveStoredName }
        return cloudFileOps.restoreCloudFileToLocal(
            driveFileId = version.driveFileId,
            originalFileName = originalName,
            versionNumber = version.versionNumber,
            targetSubfolder = parentFolder
        )
    }

    suspend fun restoreCloudBrowseItem(item: CloudBrowseItem): RestoreOutcome {
        return if (item.isFolder) {
            cloudFileOps.restoreCloudFolderRecursively(item)
        } else {
            cloudFileOps.restoreCloudFileToLocal(
                driveFileId = item.driveFileId,
                originalFileName = item.name,
                versionNumber = 1,
                mimeType = item.mimeType
            )
        }
    }

    /**
     * One-way Cloud-to-Local Folder Mirror (Rule 3: Separation of Backup & Mirroring).
     * The folder is registered so later Drive changes are mirrored automatically (incremental, non-overwriting).
     */
    suspend fun mirrorCloudFolderToLocal(item: CloudBrowseItem): RestoreOutcome {
        val outcome = cloudFileOps.mirrorCloudFolderToLocal(item)
        if (outcome.success && item.isFolder) {
            registerMirrorFolder(item)
        }
        return outcome
    }

    suspend fun toggleOfflinePin(item: CloudBrowseItem): RestoreOutcome {
        return if (item.isPinnedOffline) {
            cloudFileOps.unpinCloudFileOffline(item.driveFileId)
            RestoreOutcome(true, null, 0L, "Removed '${item.name}' from offline cache (Cloud file untouched).")
        } else if (item.isFolder) {
            cloudFileOps.pinCloudFolderOfflineRecursively(item, _settings.value.offlineCacheBudgetBytes)
        } else {
            cloudFileOps.pinCloudFileOffline(item, _settings.value.offlineCacheBudgetBytes)
        }
    }

    suspend fun getAllVersionsForFile(sourceFileIdentity: String): List<BackupFileEntity> {
        return backupDao.getAllVersionsForIdentity(sourceFileIdentity)
    }

    suspend fun getAccountBackupVersionsPaged(limit: Int = 200, offset: Int = 0): List<BackupVersionEntity> {
        return backupDao.getBackupVersionsForAccountPaged(currentAccountId(), limit, offset)
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

    suspend fun buildDashboardSummary(precomputedCoverage: List<FolderCoverageItem>? = null): DashboardSummary {
        val accountId = currentAccountId()
        val total = backupDao.countAllRecordsForAccount(accountId)
        val verified = backupDao.countVerifiedRecordsForAccount(accountId)
        val pending = backupDao.countPendingRecordsForAccount(accountId)
        val failed = backupDao.countFailedRecordsForAccount(accountId)
        val preservedDeleted = backupDao.countPreservedAfterLocalDeleteForAccount(accountId)
        val uploadedBytes = backupDao.sumTotalVerifiedUploadedBytesForAccount(accountId)
        val lastBackup = backupDao.getLastSuccessfulBackupTimestampForAccount(accountId)
        val coverage = precomputedCoverage ?: getFolderCoverageItems()
        val accessibleCount = coverage.count { it.accessStatus == FolderAccessStatus.ACCESSIBLE }
        val restrictedCount = coverage.count { it.accessStatus == FolderAccessStatus.RESTRICTED_BY_ANDROID_OS }
        val accountState = authManager.accountState.value
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
            networkError = backupEngine.transferConditionBlock.value ?: backupEngine.lastNetworkError.value,
            pendingCount = pending,
            verifiedCount = verified
        )

        val activeUpload = backupEngine.activeUploadProgress.value
        val activePhoneSource = if (activeUpload != null) {
            "/storage/emulated/0/${activeUpload.relativePath}"
        } else {
            val enabledFolderNames = coverage
                .filter { it.accessStatus == FolderAccessStatus.ACCESSIBLE && it.backupEnabled }
                .joinToString(", ") { it.relativePath }
                .ifBlank { "DCIM, Pictures, Videos, Downloads, Documents, WhatsApp/Media, Telegram" }
            "/storage/emulated/0/{$enabledFolderNames}"
        }
        val activeDriveTarget = if (activeUpload != null) {
            val parentFolder = activeUpload.relativePath.substringBeforeLast('/', "")
            if (parentFolder.isBlank()) {
                "My Drive / ${accountState.rootDriveFolderName} / ${activeUpload.fileName}"
            } else {
                "My Drive / ${accountState.rootDriveFolderName} / $parentFolder / ${activeUpload.fileName}"
            }
        } else {
            "My Drive / ${accountState.rootDriveFolderName} / <Phone_Folder_Name> / <File_Name>"
        }

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
            currentlyUploading = activeUpload,
            backgroundSyncStatus = syncStatus,
            offlinePinnedCount = offlinePinnedCount,
            offlineCacheUsedBytes = offlineUsedBytes,
            offlineCacheBudgetBytes = _settings.value.offlineCacheBudgetBytes,
            permissionFailureMessage = backupEngine.lastPermissionError.value,
            quotaFullAlert = backupEngine.quotaFullState.value,
            networkFailureMessage = backupEngine.transferConditionBlock.value ?: backupEngine.lastNetworkError.value,
            isBackupRunning = backupEngine.isRunning.value,
            currentPhoneSourcePath = activePhoneSource,
            currentDriveTargetPath = activeDriveTarget,
            currentScanningOrUploadingFile = backupEngine.currentScanActivity.value,
            isContinuousAutoBackupServiceActive = !_settings.value.backgroundSyncPaused && !backupEngine.isPaused.value
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
            networkError != null && networkError.contains("Waiting for Charging", ignoreCase = true) &&
                pendingCount > 0 -> BackgroundSyncStatus.WAITING_FOR_CHARGING
            networkError != null && (
                networkError.contains("Network", ignoreCase = true) ||
                    networkError.contains("Wi-Fi", ignoreCase = true)
                ) -> BackgroundSyncStatus.WAITING_FOR_NETWORK
            _settings.value.chargingOnly && pendingCount > 0 -> BackgroundSyncStatus.WAITING_FOR_CHARGING
            pendingCount == 0 && verifiedCount > 0 -> BackgroundSyncStatus.BACKUP_COMPLETED
            else -> BackgroundSyncStatus.WAITING_FOR_ANDROID_SCHEDULER
        }
    }

    suspend fun getFolderCoverageItems(): List<FolderCoverageItem> {
        val accountId = currentAccountId()
        val baseItems = storageSource.getFolderCoverageReport(emptyMap())
        val savedFolders = backupDao.getBackupFoldersForAccount(accountId)
        val savedByPath = savedFolders.associateBy { it.relativePath }
        val savedByName = savedFolders.associateBy { it.folderName }

        val standardMerged = baseItems.map { item ->
            val categoryKey = item.folderName.removePrefix("Dynamic Folder: ").trim()
            val backedUp = backupDao.countVerifiedFilesInCategoryForAccount(accountId, item.folderName)
                .takeIf { it > 0 }
                ?: backupDao.countVerifiedFilesInCategoryForAccount(accountId, categoryKey)
            val saved = savedByPath[item.relativePath] ?: savedByName[item.folderName]
            val enabled = if (saved != null && item.accessStatus != FolderAccessStatus.RESTRICTED_BY_ANDROID_OS) {
                saved.backupEnabled
            } else {
                item.backupEnabled
            }
            item.copy(
                backedUpFileCount = backedUp,
                backupEnabled = enabled
            )
        }
        val existingPaths = standardMerged.map { it.relativePath.lowercase() }.toSet()
        val existingNames = standardMerged.map { it.folderName.lowercase() }.toSet()
        val customExtras = savedFolders.filter { sf ->
            sf.relativePath.lowercase() !in existingPaths && sf.folderName.lowercase() !in existingNames
        }.map { sf ->
            FolderCoverageItem(
                folderName = "Custom: ${sf.folderName}",
                relativePath = sf.relativePath,
                accessStatus = runCatching { FolderAccessStatus.valueOf(sf.accessStatus) }
                    .getOrDefault(FolderAccessStatus.ACCESSIBLE),
                discoveredFileCount = sf.discoveredFileCount,
                backedUpFileCount = sf.backedUpFileCount,
                restrictionExplanation = "Custom subfolder rule -> Android_Permanent_Backup/${sf.relativePath}",
                backupEnabled = sf.backupEnabled,
                lastScanTimeMs = sf.lastScanTimeMs
            )
        }
        return standardMerged + customExtras
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
            userBackupConsentGranted = p.getBoolean("consent_granted", true),
            wifiOnly = p.getBoolean("wifi_only", false),
            chargingOnly = p.getBoolean("charging_only", false),
            unmeteredNetworkOnly = p.getBoolean("unmetered_only", false),
            uploadConcurrency = p.getInt("concurrency", 1),
            periodicIntervalHours = p.getInt("interval_hours", 6),
            chunkSizeBytes = p.getInt("chunk_size_bytes", 256 * 1024),
            offlineCacheBudgetBytes = p.getLong("offline_cache_budget_bytes", 100L * 1024 * 1024),
            backgroundSyncPaused = p.getBoolean("background_sync_paused", false),
            maxUploadSpeedKbps = p.getInt("max_upload_speed_kbps", 0)
        )
    }
}
