package com.permanentbackup.gdrive.domain.engine

import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.BackupVersionEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.DriveChangeTokenEntity
import com.permanentbackup.gdrive.data.local.DriveFolderCacheEntity
import com.permanentbackup.gdrive.data.local.FailedOperationEntity
import com.permanentbackup.gdrive.data.local.LocalFileEntity
import com.permanentbackup.gdrive.data.local.UploadSessionEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.model.ActiveUploadProgress
import com.permanentbackup.gdrive.domain.model.AuthTokenExpiredException
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DiscoveredSourceFile
import com.permanentbackup.gdrive.domain.model.DriveBadRequestException
import com.permanentbackup.gdrive.domain.model.DriveConflictException
import com.permanentbackup.gdrive.domain.model.DrivePermissionException
import com.permanentbackup.gdrive.domain.model.DriveQuotaInfo
import com.permanentbackup.gdrive.domain.model.DriveRateLimitException
import com.permanentbackup.gdrive.domain.model.DriveResourceNotFoundException
import com.permanentbackup.gdrive.domain.model.DriveServerException
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.StorageQuotaFullException
import com.permanentbackup.gdrive.domain.model.SyncJobState
import com.permanentbackup.gdrive.domain.model.UploadSessionExpiredException
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import java.io.IOException
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

data class BackupCycleSummary(
    val scannedFiles: Int,
    val newlyQueuedVersions: Int,
    val skippedUnchangedFiles: Int,
    val uploadedAndVerifiedFiles: Int,
    val failedUploads: Int,
    val preservedDeletedLocalFiles: Int,
    val permissionDenied: Boolean = false,
    val quotaFull: Boolean = false,
    val errorMessage: String? = null
)

/**
 * Desktop-Class Permanent Backup & Synchronization Engine (Sections 6, 7, 8, 9, 10):
 * 1. Strictly Upload-Only, Append-Only semantics (Never deletes or overwrites remote Drive files).
 * 2. Transactional State Machine: DISCOVERED -> QUEUED -> PREPARING -> UPLOADING -> VERIFYING -> COMPLETED
 *    with FAILED_RETRYABLE -> RETRY_SCHEDULED and FAILED_ACTION_REQUIRED.
 * 3. Pre-upload Remote Crash Reconciliation: Checks Google Drive before uploading to prevent duplicate
 *    files if a prior upload succeeded remotely right before an app crash or DB failure.
 * 4. Streaming Resumable Chunked Upload with 64 KiB RAM buffer & exact byte-offset recovery.
 * 5. Post-upload Remote Metadata, File Size & SHA-256/MD5 Checksum verification.
 * 6. Paged database processing supporting 100,000+ to 1,000,000+ records with bounded memory.
 */
class PermanentBackupEngine(
    private val backupDao: BackupDao,
    private val storageSource: StorageFileSource,
    private val driveApiClient: DriveApiClient,
    private val authManager: GoogleAuthManager,
    private val deviceConditions: DeviceConditionsProvider = AlwaysSatisfiedDeviceConditions
) {
    companion object {
        const val ROOT_DRIVE_FOLDER_NAME = "Android_Permanent_Backup"
        const val PAGINATION_BATCH_SIZE = 500
        const val UPLOAD_BATCH_SIZE = 50

        /** Identity prefix for direct uploads that target an explicit Google Drive folder ID (My Drive "Upload File"). */
        const val SHARED_DRIVE_TARGET_PREFIX = "shared:drive:"
        private const val MAX_PINNED_ANCESTOR_DEPTH = 32
    }

    private val _activeUploadProgress = MutableStateFlow<ActiveUploadProgress?>(null)
    val activeUploadProgress: StateFlow<ActiveUploadProgress?> = _activeUploadProgress.asStateFlow()

    /** Live progress of EVERY concurrently running upload worker, keyed by record ID. */
    private val _activeUploads = MutableStateFlow<Map<Long, ActiveUploadProgress>>(emptyMap())
    val activeUploads: StateFlow<Map<Long, ActiveUploadProgress>> = _activeUploads.asStateFlow()
    private val progressLock = Any()

    /** Aggregate speed of all parallel upload streams. */
    private val _currentSpeedBps = MutableStateFlow(0L)
    val currentSpeedBps: StateFlow<Long> = _currentSpeedBps.asStateFlow()

    private val _isRunning = MutableStateFlow(false)
    val isRunning: StateFlow<Boolean> = _isRunning.asStateFlow()

    private val _isPaused = MutableStateFlow(false)
    val isPaused: StateFlow<Boolean> = _isPaused.asStateFlow()

    private val _lastPermissionError = MutableStateFlow<String?>(null)
    val lastPermissionError: StateFlow<String?> = _lastPermissionError.asStateFlow()

    private val _lastNetworkError = MutableStateFlow<String?>(null)
    val lastNetworkError: StateFlow<String?> = _lastNetworkError.asStateFlow()

    private val _quotaFullState = MutableStateFlow(false)
    val quotaFullState: StateFlow<Boolean> = _quotaFullState.asStateFlow()

    private val _latestQuotaInfo = MutableStateFlow(DriveQuotaInfo())
    val latestQuotaInfo: StateFlow<DriveQuotaInfo> = _latestQuotaInfo.asStateFlow()

    /** Last reason uploads were held back by Settings (Wi-Fi only / Charging only), or null. */
    private val _transferConditionBlock = MutableStateFlow<String?>(null)
    val transferConditionBlock: StateFlow<String?> = _transferConditionBlock.asStateFlow()

    /** Real-time phone source folder/file and Google Drive destination path being scanned or backed up. */
    private val _currentScanActivity = MutableStateFlow<String?>(null)
    val currentScanActivity: StateFlow<String?> = _currentScanActivity.asStateFlow()

    /** True while verifying or creating the root backup folder on Google Drive. */
    private val _isVerifyingOrCreatingDriveFolder = MutableStateFlow(false)
    val isVerifyingOrCreatingDriveFolder: StateFlow<Boolean> = _isVerifyingOrCreatingDriveFolder.asStateFlow()

    fun activeRootFolderName(): String {
        return authManager.accountState.value.rootDriveFolderName.trim().ifBlank { ROOT_DRIVE_FOLDER_NAME }
    }

    /**
     * Optional callback invoked during incremental cloud changes sync when a pinned offline file
     * has been updated on Google Drive (or a new file appeared inside a pinned offline folder)
     * so the local offline cache is automatically refreshed.
     */
    var onPinnedCloudFileModified: (suspend (CloudBrowseItem) -> Unit)? = null

    /** Invoked after a changes poll detected at least one change (used to keep Mirror folders in sync). */
    var onCloudChangesDetected: (suspend (Int) -> Unit)? = null

    /** Invoked after a record is VERIFIED on Drive (used to clean up app-private share staging copies). */
    var onRecordVerified: (suspend (BackupFileEntity) -> Unit)? = null

    private val pausedRecordIds: MutableSet<Long> = Collections.synchronizedSet(HashSet())
    private val cycleMutex = Mutex()
    private val folderCreationMutex = Mutex()
    private val quotaRefreshMutex = Mutex()
    @Volatile
    private var activeCycleDeferred: CompletableDeferred<BackupCycleSummary>? = null
    @Volatile
    private var activeCycleJob: kotlinx.coroutines.Job? = null
    @Volatile
    private var rerunRequestedSettings: BackupSettings? = null

    fun setEnginePaused(paused: Boolean) {
        _isPaused.value = paused
    }

    /**
     * Atomically cancels any in-flight backup cycle for the previous account and resets all
     * transient engine states (quota, active upload progress, scan banner, and error messages)
     * so switching accounts immediately reflects the newly selected account ("Account switch means everything switches").
     */
    fun resetStateForAccountSwitch() {
        rerunRequestedSettings = null
        activeCycleDeferred?.cancel()
        activeCycleDeferred = null
        activeCycleJob?.cancel()
        activeCycleJob = null
        pausedRecordIds.clear()
        resetAllUploadProgress()
        _latestQuotaInfo.value = DriveQuotaInfo()
        _quotaFullState.value = false
        _lastNetworkError.value = null
        _lastPermissionError.value = null
        _transferConditionBlock.value = null
        _currentScanActivity.value = null
        _isVerifyingOrCreatingDriveFolder.value = false
        _isRunning.value = false
    }

    suspend fun pauseSingleRecord(recordId: Long, paused: Boolean) {
        if (paused) {
            pausedRecordIds.add(recordId)
            backupDao.setSyncJobPaused(recordId, true)
        } else {
            pausedRecordIds.remove(recordId)
            // Resume = eligible immediately (clears pause flag AND any exponential retry backoff)
            backupDao.setSyncJobPaused(recordId, false)
            backupDao.clearSyncJobRetryBackoff(recordId)
        }
    }

    fun isRecordPaused(recordId: Long): Boolean = recordId in pausedRecordIds

    private fun publishUploadProgress(progress: ActiveUploadProgress) {
        synchronized(progressLock) {
            val updated = _activeUploads.value + (progress.recordId to progress)
            _activeUploads.value = updated
            _activeUploadProgress.value = progress
            _currentSpeedBps.value = updated.values.sumOf { it.transferSpeedBps }
        }
    }

    private fun clearUploadProgress(recordId: Long) {
        synchronized(progressLock) {
            val updated = _activeUploads.value - recordId
            _activeUploads.value = updated
            _activeUploadProgress.value = updated.values.lastOrNull()
            _currentSpeedBps.value = updated.values.sumOf { it.transferSpeedBps }
        }
    }

    private fun resetAllUploadProgress() {
        synchronized(progressLock) {
            _activeUploads.value = emptyMap()
            _activeUploadProgress.value = null
            _currentSpeedBps.value = 0L
        }
    }

    /**
     * Re-evaluates Settings → "Wi-Fi only" / "Charging only" against the live device state.
     * Publishes the user-facing block reason to [transferConditionBlock] (or clears it).
     */
    fun checkTransferConditions(settings: BackupSettings): TransferConditionResult {
        val result = try {
            deviceConditions.evaluate(settings)
        } catch (e: Exception) {
            TransferConditionResult(true)
        }
        _transferConditionBlock.value = if (result.allowed) null else result.blockReason
        return result
    }

    /**
     * Explicit Google Drive folder ID encoded in a direct-upload identity
     * (`shared:drive:<folderId>/<fileName>`), or null for regular mirrored-hierarchy backups.
     */
    fun explicitDriveTargetFolderId(sourceFileIdentity: String): String? {
        if (!sourceFileIdentity.startsWith(SHARED_DRIVE_TARGET_PREFIX)) return null
        return sourceFileIdentity.removePrefix(SHARED_DRIVE_TARGET_PREFIX)
            .substringBefore('/')
            .trim()
            .ifBlank { null }
    }

    /**
     * Recovers any IN_PROGRESS tasks left incomplete due to an app crash or device reboot,
     * preserving their resumableSessionUri and uploadedBytes progress.
     */
    suspend fun recoverInterruptedUploadsAfterRestart(): Int {
        val accountId = currentAccountId()
        val interrupted = backupDao.getInProgressRecordsForAccount(accountId)
        for (record in interrupted) {
            backupDao.updateUploadProgress(
                id = record.id,
                uploadedBytes = record.uploadedBytes,
                sessionUri = record.resumableSessionUri,
                status = UploadStatus.PENDING_RETRY
            )
            backupDao.transitionSyncJobState(
                accountId = accountId,
                recordId = record.id,
                sourceFileIdentity = record.sourceFileIdentity,
                fileName = record.driveStoredName,
                relativePath = record.relativePath,
                backupVersion = record.backupVersion,
                newState = SyncJobState.RETRY_SCHEDULED,
                uploadedBytes = record.uploadedBytes,
                totalBytes = record.fileSize,
                errorMessage = "Recovered interrupted upload after crash/restart at offset ${record.uploadedBytes}"
            )
        }
        return interrupted.size
    }

    /**
     * Connects to Google Drive API v3, refreshes storage quota & authenticated user profile, and verifies
     * whether the active root backup folder actually exists on Google Drive (or creates it if
     * [createFolderIfMissing] is true). Dynamically reconciles any SQLite records that belonged to a
     * foreign root folder without any hardcoded emails or folder IDs.
     */
    suspend fun refreshRemoteQuotaAndRootFolder(createFolderIfMissing: Boolean = false): Boolean =
        quotaRefreshMutex.withLock {
            if (!authManager.accountState.value.isConnected) return@withLock false
            _isVerifyingOrCreatingDriveFolder.value = true
            return@withLock try {
                var token = authManager.getValidAccessToken()
                val userAndQuota = try {
                    driveApiClient.fetchDriveUserAndQuota(token)
                } catch (e: AuthTokenExpiredException) {
                    token = authManager.invalidateAndRefreshTokenOn401()
                    driveApiClient.fetchDriveUserAndQuota(token)
                }
                if (!authManager.updateVerifiedUserProfile(userAndQuota.email, userAndQuota.displayName)) {
                    return@withLock false
                }
                val quota = userAndQuota.quota
                _latestQuotaInfo.value = quota
                _quotaFullState.value = quota.isQuotaFull

                val rootName = activeRootFolderName()
                val verifiedRootId = if (createFolderIfMissing) {
                    driveApiClient.ensureFolderExists(
                        accessToken = token,
                        folderName = rootName,
                        parentFolderId = null
                    )
                } else {
                    driveApiClient.findFolderByName(
                        accessToken = token,
                        folderName = rootName,
                        parentFolderId = null
                    )
                }
                if (!verifiedRootId.isNullOrBlank()) {
                    reconcileForeignRootRecordsForAccount(currentAccountId(), verifiedRootId)
                }
                authManager.setRootDriveFolderName(rootName, verifiedRootId)
                _lastNetworkError.value = null
                true
            } catch (e: DriveRateLimitException) {
                val alreadyHasCreatedFolder = !authManager.accountState.value.rootDriveFolderId.isNullOrBlank()
                if (!alreadyHasCreatedFolder) {
                    _lastNetworkError.value = e.message
                }
                alreadyHasCreatedFolder
            } catch (e: Exception) {
                _lastNetworkError.value = e.message
                false
            } finally {
                _isVerifyingOrCreatingDriveFolder.value = false
            }
        }

    /**
     * Dynamically detects if [accountId] previously had SQLite records or folder cache entries created
     * under a foreign/different root folder ID (`oldRootId != verifiedRootId`), and reassigns them to
     * whichever saved account actually owns `oldRootId` (or archives them non-destructively) so [accountId]
     * cleanly backs up all local files into its own [verifiedRootId]. Zero hardcoded emails or folder IDs.
     */
    suspend fun reconcileForeignRootRecordsForAccount(accountId: String, verifiedRootId: String) {
        if (accountId.isBlank() || verifiedRootId.isBlank()) return
        try {
            val allDbAccounts = backupDao.getAllGoogleAccounts()
            val currentDbAccount = allDbAccounts.find { it.accountId.equals(accountId, ignoreCase = true) }
            val previousDbRootId = currentDbAccount?.backupRootFolderId?.trim().orEmpty()

            val cachedEntries = backupDao.getFolderCacheEntriesByPrefix("$accountId:")
            val cachedRootIds = cachedEntries.mapNotNull { entry ->
                val rest = entry.relativePath.removePrefix("$accountId:")
                val candidateRoot = rest.substringBefore(':', "")
                candidateRoot.takeIf { it.isNotBlank() }
            }.toSet()

            val foreignRootIds = buildSet {
                if (previousDbRootId.isNotBlank() && previousDbRootId != verifiedRootId) {
                    add(previousDbRootId)
                }
                for (cid in cachedRootIds) {
                    if (cid != verifiedRootId) add(cid)
                }
            }

            if (foreignRootIds.isEmpty()) return

            for (foreignRootId in foreignRootIds) {
                val ownerAccount = allDbAccounts.firstOrNull {
                    !it.accountId.equals(accountId, ignoreCase = true) &&
                        it.backupRootFolderId == foreignRootId
                }
                val targetOwnerId = ownerAccount?.accountId ?: "archived_root:$foreignRootId"
                backupDao.clearSpoofedRootFolderIdFromAccounts(targetOwnerId, foreignRootId)
                backupDao.reassignSpoofedBackupRecordsToOwner(accountId, targetOwnerId)
                backupDao.archiveRemainingSpoofedBackupRecords(accountId, targetOwnerId)
                backupDao.reassignSpoofedBackupVersionsToOwner(accountId, targetOwnerId)
                backupDao.archiveRemainingSpoofedBackupVersions(accountId, targetOwnerId)
                backupDao.reassignSpoofedSyncJobsToOwner(accountId, targetOwnerId)
                backupDao.reassignSpoofedSyncEventsToOwner(accountId, targetOwnerId)
                backupDao.reassignSpoofedFailedOperationsToOwner(accountId, targetOwnerId)
                backupDao.reassignSpoofedUploadSessionsToOwner(accountId, targetOwnerId)
                backupDao.archiveRemainingSpoofedUploadSessions(accountId, targetOwnerId)
                backupDao.reassignSpoofedCloudFilesToOwner(accountId, targetOwnerId)
                backupDao.archiveSpoofedDriveFolderCache("$accountId:$foreignRootId:", foreignRootId)
            }
        } catch (_: Throwable) {
            // Non-blocking dynamic reconciliation
        }
    }

    /**
     * Explicitly creates or switches/renames the target Google Drive backup folder.
     * - When connected to Google Drive, immediately creates/resolves the folder on Drive and stores its real folder ID.
     * - When not yet connected, saves the chosen folder name with `rootDriveFolderId = null` so it will be created upon connection.
     */
    suspend fun createOrSwitchRootBackupFolder(newFolderName: String): Result<String> {
        val cleanName = newFolderName.trim()
            .replace("/", "_")
            .replace("\\", "_")
            .ifBlank { ROOT_DRIVE_FOLDER_NAME }
        if (!authManager.accountState.value.isConnected) {
            authManager.setRootDriveFolderName(cleanName, null)
            return Result.success("")
        }
        _isVerifyingOrCreatingDriveFolder.value = true
        return try {
            var token = authManager.getValidAccessToken()
            val rootId = try {
                driveApiClient.ensureFolderExists(
                    accessToken = token,
                    folderName = cleanName,
                    parentFolderId = null
                )
            } catch (e: AuthTokenExpiredException) {
                token = authManager.invalidateAndRefreshTokenOn401()
                driveApiClient.ensureFolderExists(
                    accessToken = token,
                    folderName = cleanName,
                    parentFolderId = null
                )
            }
            authManager.setRootDriveFolderName(cleanName, rootId)
            _lastNetworkError.value = null
            _currentScanActivity.value =
                "✓ Google Drive folder '$cleanName' is created & ready at My Drive/$cleanName"
            Result.success(rootId)
        } catch (e: Exception) {
            _lastNetworkError.value = e.message
            Result.failure(e)
        } finally {
            _isVerifyingOrCreatingDriveFolder.value = false
        }
    }

    /**
     * Incremental Google Drive Changes API v3 sync across all available pages (Section 9).
     * - Indexes new/modified Drive files in the local `cloud_files` cache.
     * - Hides files reported `removed`/`trashed` from the LOCAL cache only (zero cloud deletion).
     * - Automatically refreshes pinned offline files modified on Drive and auto-pins new files that
     *   appear inside a folder the user pinned with "Keep Folder Offline".
     * - Notifies [onCloudChangesDetected] so one-way Mirror folders stay in sync.
     */
    suspend fun syncCloudChangesIncremental(): Int {
        if (!authManager.accountState.value.isConnected) return 0
        return try {
            val token = authManager.getValidAccessToken()
            val accountId = currentAccountId()
            val existingTokenEntity = backupDao.getDriveChangeToken(accountId)
            if (existingTokenEntity == null) {
                val startToken = driveApiClient.fetchStartPageToken(token)
                backupDao.upsertDriveChangeToken(
                    DriveChangeTokenEntity(
                        accountId = accountId,
                        startPageToken = startToken
                    )
                )
                return 0
            }

            var currentPageToken: String? = existingTokenEntity.startPageToken
            var latestStartPageToken: String? = null
            var totalChanged = 0
            var pageCount = 0
            val modifiedPinnedItems = mutableListOf<CloudBrowseItem>()
            val pinnedAncestorCache = HashMap<String, Boolean>()

            while (!currentPageToken.isNullOrBlank() && pageCount < 10) {
                pageCount++
                val changesPage = driveApiClient.fetchDriveChanges(
                    accessToken = token,
                    accountId = accountId,
                    pageToken = currentPageToken
                )
                if (changesPage.changedFiles.isNotEmpty()) {
                    val entities = changesPage.changedFiles.map { item ->
                        val prev = backupDao.getCloudFileById(item.driveFileId)
                        val isPinned = prev?.isPinnedOffline ?: false
                        if (!item.isFolder) {
                            if (isPinned) {
                                val modifiedChanged = item.modifiedTimeMs > 0L && item.modifiedTimeMs != prev?.modifiedTimeMs
                                val checksumChanged = !item.md5Checksum.isNullOrBlank() && item.md5Checksum != prev?.md5Checksum
                                if (modifiedChanged || checksumChanged || prev?.trashed == true) {
                                    modifiedPinnedItems.add(item.copy(isPinnedOffline = true))
                                }
                            } else if (isInsidePinnedOfflineFolder(item.parentFolderId, pinnedAncestorCache)) {
                                // New/updated file inside a "Keep Folder Offline" folder -> auto-pin
                                modifiedPinnedItems.add(item)
                            }
                        }
                        CloudFileEntity(
                            driveFileId = item.driveFileId,
                            accountId = accountId,
                            name = item.name,
                            mimeType = item.mimeType,
                            sizeBytes = item.sizeBytes,
                            md5Checksum = item.md5Checksum,
                            sha256Checksum = item.sha256Checksum,
                            parentFolderId = item.parentFolderId,
                            relativePath = prev?.relativePath,
                            isFolder = item.isFolder,
                            isProtectedBackupFile = item.isProtectedBackupFile,
                            starred = item.starred,
                            shared = item.shared,
                            webViewLink = item.webViewLink,
                            createdTimeMs = item.createdTimeMs,
                            modifiedTimeMs = item.modifiedTimeMs,
                            isPinnedOffline = isPinned,
                            offlineLocalPath = prev?.offlineLocalPath,
                            offlineCachedBytes = prev?.offlineCachedBytes ?: 0L
                        )
                    }
                    backupDao.upsertCloudFiles(entities)
                    totalChanged += changesPage.changedFiles.size
                }
                if (changesPage.removedFileIds.isNotEmpty()) {
                    // LOCAL cache hygiene only: hides ghost entries in My Drive / virtual drive.
                    // Nothing is deleted or trashed on Google Drive, and pinned bytes stay on disk
                    // so an un-trash on Drive restores the entry seamlessly.
                    backupDao.markCloudFilesRemovedLocally(changesPage.removedFileIds.distinct())
                    totalChanged += changesPage.removedFileIds.size
                }
                if (!changesPage.newStartPageToken.isNullOrBlank()) {
                    latestStartPageToken = changesPage.newStartPageToken
                }
                currentPageToken = changesPage.nextPageToken
                if (!currentPageToken.isNullOrBlank()) {
                    latestStartPageToken = currentPageToken
                }
            }

            val updatedToken = latestStartPageToken ?: existingTokenEntity.startPageToken
            backupDao.upsertDriveChangeToken(
                DriveChangeTokenEntity(
                    accountId = accountId,
                    startPageToken = updatedToken,
                    lastChangesPollMs = System.currentTimeMillis(),
                    changesDetectedCount = existingTokenEntity.changesDetectedCount + totalChanged
                )
            )

            // Refresh modified pinned offline files / auto-pin new files in pinned folders
            for (pinnedItem in modifiedPinnedItems) {
                try {
                    onPinnedCloudFileModified?.invoke(pinnedItem)
                } catch (e: Exception) {
                    if (e is CancellationException) throw e
                }
            }

            if (totalChanged > 0) {
                try {
                    onCloudChangesDetected?.invoke(totalChanged)
                } catch (e: Exception) {
                    if (e is CancellationException) throw e
                }
            }

            totalChanged
        } catch (e: Exception) {
            if (e is CancellationException) throw e
            0
        }
    }

    private suspend fun isInsidePinnedOfflineFolder(parentFolderId: String?, cache: MutableMap<String, Boolean>): Boolean {
        val visited = ArrayList<String>()
        var nextId: String? = parentFolderId
        var depth = 0
        var result = false
        while (depth < MAX_PINNED_ANCESTOR_DEPTH) {
            val id = nextId
            if (id.isNullOrBlank()) break
            val cached = cache[id]
            if (cached != null) {
                result = cached
                break
            }
            visited.add(id)
            val entity = backupDao.getCloudFileById(id) ?: break
            if (entity.isFolder && entity.isPinnedOffline && !entity.trashed) {
                result = true
                break
            }
            nextId = entity.parentFolderId
            depth++
        }
        visited.forEach { cache[it] = result }
        return result
    }

    /**
     * Executes a complete scan + deduplication + version detection + resumable upload cycle.
     * Coalesces concurrent invocations so two callers never scan or upload in parallel.
     *
     * @param respectRetryBackoff when true (automatic/background triggers), records in RETRY_SCHEDULED
     *   whose exponential `nextRetryAtMs` is still in the future are skipped. Manual runs retry immediately.
     */
    suspend fun runBackupCycle(
        settings: BackupSettings,
        respectRetryBackoff: Boolean = false
    ): BackupCycleSummary = runCoalesced(settings) {
        executeBackupCycleInternal(settings, respectRetryBackoff)
    }

    /**
     * Uploads already-queued records only (no full storage rescan). Used by single-file "Backup File",
     * share-sheet uploads, the real-time watcher (after targeted queuing), Resume and the SAF provider.
     */
    suspend fun runUploadQueueOnly(
        settings: BackupSettings,
        respectRetryBackoff: Boolean = false
    ): BackupCycleSummary = runCoalesced(settings) {
        executeUploadOnlyInternal(settings, respectRetryBackoff)
    }

    private suspend fun runCoalesced(
        settings: BackupSettings,
        block: suspend () -> BackupCycleSummary
    ): BackupCycleSummary {
        activeCycleDeferred?.let { inFlight ->
            if (inFlight.isActive) {
                // A cycle is already running: ask it to make one more upload pass for anything queued
                // after it started (e.g. a Resume tap or a watcher event), then share its result.
                rerunRequestedSettings = settings
                return inFlight.await()
            }
        }
        return cycleMutex.withLock {
            val deferred = CompletableDeferred<BackupCycleSummary>()
            activeCycleDeferred = deferred
            activeCycleJob = kotlin.coroutines.coroutineContext[kotlinx.coroutines.Job]
            try {
                rerunRequestedSettings = null
                var summary = block()
                var extraPasses = 0
                while (extraPasses < 3 && !_isPaused.value) {
                    val pendingRerun = rerunRequestedSettings ?: break
                    rerunRequestedSettings = null
                    extraPasses++
                    val extra = executeUploadOnlyInternal(pendingRerun, respectRetryBackoff = false)
                    summary = summary.copy(
                        uploadedAndVerifiedFiles = summary.uploadedAndVerifiedFiles + extra.uploadedAndVerifiedFiles,
                        failedUploads = summary.failedUploads + extra.failedUploads,
                        quotaFull = summary.quotaFull || extra.quotaFull
                    )
                }
                deferred.complete(summary)
                summary
            } catch (t: Throwable) {
                deferred.completeExceptionally(t)
                throw t
            } finally {
                activeCycleDeferred = null
                activeCycleJob = null
            }
        }
    }

    private suspend fun executeUploadOnlyInternal(
        settings: BackupSettings,
        respectRetryBackoff: Boolean
    ): BackupCycleSummary {
        if (!settings.userBackupConsentGranted) {
            val msg = "User backup consent not granted yet. No files were scanned or read."
            _lastPermissionError.value = msg
            return BackupCycleSummary(0, 0, 0, 0, 0, 0, permissionDenied = true, errorMessage = msg)
        }
        if (settings.backgroundSyncPaused || _isPaused.value) {
            return BackupCycleSummary(
                0, 0, 0, 0, 0,
                preservedDeletedLocalFiles = backupDao.countPreservedAfterLocalDeleteForAccount(currentAccountId()),
                errorMessage = "Backup is currently paused by user."
            )
        }
        if (!authManager.accountState.value.isConnected) {
            return BackupCycleSummary(
                0, 0, 0, 0, 0,
                preservedDeletedLocalFiles = backupDao.countPreservedAfterLocalDeleteForAccount(currentAccountId()),
                errorMessage = "Google Account is not connected. Queued files are kept safely."
            )
        }
        _isRunning.value = true
        try {
            val outcome = processPendingUploadQueue(settings, respectRetryBackoff)
            return BackupCycleSummary(
                scannedFiles = 0,
                newlyQueuedVersions = 0,
                skippedUnchangedFiles = 0,
                uploadedAndVerifiedFiles = outcome.first,
                failedUploads = outcome.second,
                preservedDeletedLocalFiles = backupDao.countPreservedAfterLocalDeleteForAccount(currentAccountId()),
                quotaFull = _quotaFullState.value,
                errorMessage = _transferConditionBlock.value ?: _lastNetworkError.value
            )
        } finally {
            resetAllUploadProgress()
            _isRunning.value = false
        }
    }

    private suspend fun executeBackupCycleInternal(
        settings: BackupSettings,
        respectRetryBackoff: Boolean
    ): BackupCycleSummary {
        if (!settings.userBackupConsentGranted) {
            val msg = "User backup consent not granted yet. No files were scanned or read."
            _lastPermissionError.value = msg
            return BackupCycleSummary(0, 0, 0, 0, 0, 0, permissionDenied = true, errorMessage = msg)
        }

        if (!storageSource.hasStoragePermission) {
            val hasPendingDirectShares = backupDao
                .getPendingRecordsForAccountPaged(currentAccountId(), limit = 20, offset = 0)
                .any { it.sourceFileIdentity.startsWith("shared:") }
            if (!hasPendingDirectShares) {
                val msg = "Storage access permission was revoked or denied. Please grant storage access."
                _lastPermissionError.value = msg
                return BackupCycleSummary(0, 0, 0, 0, 0, 0, permissionDenied = true, errorMessage = msg)
            }
        }

        if (settings.backgroundSyncPaused || _isPaused.value) {
            return BackupCycleSummary(
                scannedFiles = 0,
                newlyQueuedVersions = 0,
                skippedUnchangedFiles = 0,
                uploadedAndVerifiedFiles = 0,
                failedUploads = 0,
                preservedDeletedLocalFiles = backupDao.countPreservedAfterLocalDeleteForAccount(currentAccountId()),
                errorMessage = "Backup is currently paused by user."
            )
        }

        _lastPermissionError.value = null
        _isRunning.value = true

        try {
            // Step 1: Recover any interrupted tasks from previous crash/reboot
            recoverInterruptedUploadsAfterRestart()

            // Load per-folder backup enable/disable rules for the active account
            val accountId = currentAccountId()
            val disabledFolders = backupDao.getBackupFoldersForAccount(accountId).filter { !it.backupEnabled }
            val disabledCategories = disabledFolders.map { it.folderName }.toSet()
            val disabledPaths = disabledFolders
                .map { it.relativePath.replace('\\', '/').trim('/').lowercase() }
                .filter { it.isNotBlank() }
                .toSet()

            // Step 2: Fast Metadata-First Incremental Scan:
            // Scan file metadata first (computeHashes = false); evaluateAndQueueDiscoveredFile reuses
            // cached SHA-256/MD5 when (fileSize, lastModifiedMs) match LocalFileEntity, and only
            // computes streaming hashes for new or modified files.
            var scannedCount = 0
            var newlyQueued = 0
            var skippedUnchanged = 0
            val discoveredIdentities = HashSet<String>(4096)

            for (discovered in storageSource.scanFilesSequence(computeHashes = false)) {
                scannedCount++
                discoveredIdentities.add(discovered.sourceFileIdentity)
                val liveRootName = activeRootFolderName()
                val liveFolderCreated = authManager.accountState.value.isConnected &&
                    !authManager.accountState.value.rootDriveFolderId.isNullOrBlank()
                if (scannedCount == 1 || scannedCount % 25 == 0) {
                    _currentScanActivity.value = if (liveFolderCreated) {
                        "From Phone: /storage/emulated/0/${discovered.relativePath} → To Google Drive: My Drive/$liveRootName/${discovered.relativePath}"
                    } else {
                        "From Phone: /storage/emulated/0/${discovered.relativePath} → Target: My Drive/$liveRootName/${discovered.relativePath}"
                    }
                }

                val normalizedRelPath = discovered.relativePath.replace('\\', '/').trim('/').lowercase()
                val isFolderDisabled = discovered.folderCategory in disabledCategories ||
                    disabledPaths.any { dp -> normalizedRelPath == dp || normalizedRelPath.startsWith("$dp/") }
                if (isFolderDisabled) {
                    skippedUnchanged++
                    continue
                }

                val queued = evaluateAndQueueDiscoveredFile(discovered)
                if (queued) {
                    newlyQueued++
                    if (newlyQueued == 1 || newlyQueued % 10 == 0) {
                        _currentScanActivity.value = if (liveFolderCreated) {
                            "Queued from Phone: /storage/emulated/0/${discovered.relativePath} → To Google Drive: My Drive/$liveRootName/${discovered.relativePath}"
                        } else {
                            "Queued from Phone: /storage/emulated/0/${discovered.relativePath} → Target: My Drive/$liveRootName/${discovered.relativePath}"
                        }
                    }
                } else {
                    skippedUnchanged++
                }
            }

            val finalRootName = activeRootFolderName()
            val finalFolderCreated = authManager.accountState.value.isConnected &&
                !authManager.accountState.value.rootDriveFolderId.isNullOrBlank()
            if (scannedCount > 0 && newlyQueued == 0) {
                _currentScanActivity.value = if (finalFolderCreated) {
                    "Verified $scannedCount phone files → Google Drive: My Drive/$finalRootName/ (100% Up to Date)"
                } else {
                    "Indexed $scannedCount phone files → Target: My Drive/$finalRootName/"
                }
            }

            // Step 3: Reconcile locally deleted files — NEVER delete from Google Drive!
            val preservedDeletedCount = if (storageSource.hasStoragePermission) {
                reconcileLocallyDeletedFiles(discoveredIdentities)
            } else {
                backupDao.countPreservedAfterLocalDeleteForAccount(accountId)
            }

            // Step 4: Execute pending uploads & incremental cloud changes sync if Google Account is connected
            if (!authManager.accountState.value.isConnected) {
                return BackupCycleSummary(
                    scannedFiles = scannedCount,
                    newlyQueuedVersions = newlyQueued,
                    skippedUnchangedFiles = skippedUnchanged,
                    uploadedAndVerifiedFiles = 0,
                    failedUploads = 0,
                    preservedDeletedLocalFiles = preservedDeletedCount,
                    errorMessage = "Google Account is not connected. Discovered files are queued safely."
                )
            }

            val uploadOutcome = processPendingUploadQueue(settings, respectRetryBackoff)

            // Step 5: Poll Google Drive Changes API v3 incrementally & auto-refresh modified pinned offline files
            syncCloudChangesIncremental()

            return BackupCycleSummary(
                scannedFiles = scannedCount,
                newlyQueuedVersions = newlyQueued,
                skippedUnchangedFiles = skippedUnchanged,
                uploadedAndVerifiedFiles = uploadOutcome.first,
                failedUploads = uploadOutcome.second,
                preservedDeletedLocalFiles = preservedDeletedCount,
                quotaFull = _quotaFullState.value,
                errorMessage = _transferConditionBlock.value ?: _lastNetworkError.value
            )
        } finally {
            resetAllUploadProgress()
            _isRunning.value = false
        }
    }

    /**
     * Evaluates a single discovered file against the Room database with per-account isolation
     * and metadata-first hash caching:
     * - Unchanged file (already backed up for the active account) -> skipped (returns false)
     * - Unchanged file not yet backed up for the active account -> queued as v1 for active account (returns true)
     * - New file -> queued as version 1 for active account (returns true)
     * - Modified file -> previous verified version preserved untouched; new version (N+1) queued (returns true)
     */
    suspend fun evaluateAndQueueDiscoveredFile(rawDiscovered: DiscoveredSourceFile): Boolean {
        val accountId = currentAccountId()
        val latestForAccount = backupDao.getLatestVersionForAccountAndIdentity(accountId, rawDiscovered.sourceFileIdentity)

        // Zero-delay fast path: if the active account already has a record with identical size & mtime
        // (and no conflicting precomputed SHA-256), skip without any disk read or hashing.
        if (latestForAccount != null &&
            latestForAccount.accountId == accountId &&
            latestForAccount.fileSize == rawDiscovered.fileSize &&
            latestForAccount.lastModifiedMs == rawDiscovered.lastModifiedMs &&
            (rawDiscovered.sha256Hash == null ||
                latestForAccount.sha256Hash == null ||
                latestForAccount.sha256Hash.equals(rawDiscovered.sha256Hash, ignoreCase = true))
        ) {
            if (!latestForAccount.localExists) {
                backupDao.updateRecord(latestForAccount.copy(localExists = true))
            }
            return latestForAccount.uploadStatus != UploadStatus.VERIFIED
        }

        // Fast metadata-first hash resolution: reuse cached SHA-256/MD5 if file size & mtime are unchanged,
        // and defer full-stream hashing of large new files (> 256 KiB) to uploadSingleRecordResumableInternal
        // so scanning thousands of phone files completes in milliseconds.
        val needSyncHashToCompareExisting = latestForAccount != null &&
            latestForAccount.fileSize == rawDiscovered.fileSize &&
            latestForAccount.sha256Hash != null
        val maxSyncScanHashBytes = if (needSyncHashToCompareExisting) 64L * 1024L * 1024L else 256L * 1024L

        val discovered = if (rawDiscovered.sha256Hash != null && rawDiscovered.md5Hash != null) {
            rawDiscovered
        } else {
            val cachedLocal = backupDao.getLocalFileByIdentity(rawDiscovered.sourceFileIdentity)
            if (cachedLocal != null &&
                cachedLocal.localExists &&
                cachedLocal.fileSize == rawDiscovered.fileSize &&
                cachedLocal.lastModifiedMs == rawDiscovered.lastModifiedMs &&
                !cachedLocal.latestSha256.isNullOrBlank() &&
                !cachedLocal.latestMd5.isNullOrBlank()
            ) {
                rawDiscovered.copy(
                    sha256Hash = cachedLocal.latestSha256,
                    md5Hash = cachedLocal.latestMd5
                )
            } else if (rawDiscovered.fileSize <= maxSyncScanHashBytes) {
                try {
                    val hashes = SourceFileReadOnlyGuard.computeStreamingHashes {
                        storageSource.openInputStream(rawDiscovered.sourceUri)
                    }
                    rawDiscovered.copy(
                        sha256Hash = hashes.sha256Hex,
                        md5Hash = hashes.md5Hex
                    )
                } catch (_: Exception) {
                    rawDiscovered
                }
            } else {
                rawDiscovered
            }
        }

        if (latestForAccount == null) {
            val recordId = backupDao.insertRecord(
                BackupFileEntity(
                    accountId = accountId,
                    sourceUri = discovered.sourceUri,
                    sourceFileIdentity = discovered.sourceFileIdentity,
                    relativePath = discovered.relativePath,
                    displayName = discovered.displayName,
                    folderCategory = discovered.folderCategory,
                    fileSize = discovered.fileSize,
                    lastModifiedMs = discovered.lastModifiedMs,
                    mimeType = discovered.mimeType,
                    sha256Hash = discovered.sha256Hash,
                    md5Hash = discovered.md5Hash,
                    driveStoredName = discovered.displayName,
                    backupVersion = 1,
                    uploadStatus = UploadStatus.PENDING,
                    localExists = true
                )
            )
            backupDao.upsertLocalFile(
                LocalFileEntity(
                    sourceFileIdentity = discovered.sourceFileIdentity,
                    sourceUri = discovered.sourceUri,
                    relativePath = discovered.relativePath,
                    displayName = discovered.displayName,
                    folderCategory = discovered.folderCategory,
                    fileSize = discovered.fileSize,
                    lastModifiedMs = discovered.lastModifiedMs,
                    mimeType = discovered.mimeType,
                    latestSha256 = discovered.sha256Hash,
                    latestMd5 = discovered.md5Hash,
                    latestVersion = 1,
                    localExists = true
                )
            )
            backupDao.transitionSyncJobState(
                accountId = accountId,
                recordId = recordId,
                sourceFileIdentity = discovered.sourceFileIdentity,
                fileName = discovered.displayName,
                relativePath = discovered.relativePath,
                backupVersion = 1,
                newState = SyncJobState.QUEUED,
                uploadedBytes = 0L,
                totalBytes = discovered.fileSize
            )
            return true
        }

        val isSameContent = isUnchangedFile(latestForAccount, discovered)
        val existingVersions = backupDao.getBackupVersionsForIdentity(discovered.sourceFileIdentity)
        val accountHasRecord = (latestForAccount.accountId == accountId && accountId != "default_account") ||
            existingVersions.isEmpty() ||
            existingVersions.any { it.accountId == accountId }

        if (isSameContent && accountHasRecord) {
            if (!latestForAccount.localExists || latestForAccount.accountId != accountId) {
                backupDao.updateRecord(latestForAccount.copy(localExists = true, accountId = accountId))
            }
            return latestForAccount.uploadStatus != UploadStatus.VERIFIED
        }

        val nextVersion = if (isSameContent && !accountHasRecord) 1 else latestForAccount.backupVersion + 1
        val isFirstBackupForThisAccount = isSameContent && !accountHasRecord
        val versionedDriveName = if (isFirstBackupForThisAccount) {
            discovered.displayName
        } else {
            buildVersionedDriveFileName(
                originalName = discovered.displayName,
                version = nextVersion,
                sha256Hash = discovered.sha256Hash,
                lastModifiedMs = discovered.lastModifiedMs
            )
        }

        val recordId = backupDao.insertRecord(
            BackupFileEntity(
                accountId = accountId,
                sourceUri = discovered.sourceUri,
                sourceFileIdentity = discovered.sourceFileIdentity,
                relativePath = discovered.relativePath,
                displayName = discovered.displayName,
                folderCategory = discovered.folderCategory,
                fileSize = discovered.fileSize,
                lastModifiedMs = discovered.lastModifiedMs,
                mimeType = discovered.mimeType,
                sha256Hash = discovered.sha256Hash,
                md5Hash = discovered.md5Hash,
                driveStoredName = versionedDriveName,
                backupVersion = nextVersion,
                uploadStatus = UploadStatus.PENDING,
                localExists = true
            )
        )
        backupDao.upsertLocalFile(
            LocalFileEntity(
                sourceFileIdentity = discovered.sourceFileIdentity,
                sourceUri = discovered.sourceUri,
                relativePath = discovered.relativePath,
                displayName = discovered.displayName,
                folderCategory = discovered.folderCategory,
                fileSize = discovered.fileSize,
                lastModifiedMs = discovered.lastModifiedMs,
                mimeType = discovered.mimeType,
                latestSha256 = discovered.sha256Hash,
                latestMd5 = discovered.md5Hash,
                latestVersion = nextVersion,
                localExists = true
            )
        )
        backupDao.transitionSyncJobState(
            accountId = accountId,
            recordId = recordId,
            sourceFileIdentity = discovered.sourceFileIdentity,
            fileName = versionedDriveName,
            relativePath = discovered.relativePath,
            backupVersion = nextVersion,
            newState = SyncJobState.QUEUED,
            uploadedBytes = 0L,
            totalBytes = discovered.fileSize
        )
        return true
    }

    private fun isUnchangedFile(existing: BackupFileEntity, discovered: DiscoveredSourceFile): Boolean {
        if (existing.fileSize != discovered.fileSize) return false
        if (existing.sha256Hash != null && discovered.sha256Hash != null) {
            return existing.sha256Hash.equals(discovered.sha256Hash, ignoreCase = true)
        }
        return existing.lastModifiedMs == discovered.lastModifiedMs
    }

    /**
     * Generates a distinct non-overwriting filename for version N+1 on Google Drive
     * (e.g., "notes__v2_9f86d081.txt") so version 1 ("notes.txt") is NEVER overwritten.
     */
    fun buildVersionedDriveFileName(
        originalName: String,
        version: Int,
        sha256Hash: String?,
        lastModifiedMs: Long
    ): String {
        if (version <= 1) return originalName
        val suffixTag = sha256Hash?.take(8) ?: lastModifiedMs.toString().takeLast(8)
        val dotIndex = originalName.lastIndexOf('.')
        return if (dotIndex > 0) {
            val base = originalName.substring(0, dotIndex)
            val ext = originalName.substring(dotIndex)
            "${base}__v${version}_${suffixTag}${ext}"
        } else {
            "${originalName}__v${version}_${suffixTag}"
        }
    }

    /**
     * Marks locally deleted files as localExists = false in Room without EVER deleting or modifying
     * their corresponding Google Drive backups.
     */
    private suspend fun reconcileLocallyDeletedFiles(presentIdentities: Set<String>): Int {
        val accountId = currentAccountId()
        var offset = 0
        val missingIdentities = mutableListOf<String>()
        while (true) {
            val page = backupDao.getActiveIdentitiesForAccountPaged(accountId, PAGINATION_BATCH_SIZE, offset)
            if (page.isEmpty()) break
            for (identity in page) {
                if (!identity.startsWith("shared:") && identity !in presentIdentities) {
                    missingIdentities.add(identity)
                }
            }
            offset += page.size
        }

        for (identity in missingIdentities) {
            backupDao.markLocalFileDeletedForAccountPreservingCloudBackup(accountId, identity)
            backupDao.markLocalFileIndexDeleted(identity)
            backupDao.markVersionsPreservedForAccountAfterLocalDelete(accountId, identity)
        }
        return backupDao.countPreservedAfterLocalDeleteForAccount(accountId)
    }

    private suspend fun processPendingUploadQueue(
        settings: BackupSettings,
        respectRetryBackoff: Boolean = false
    ): Pair<Int, Int> {
        var verifiedCount = 0
        var failedCount = 0

        // Engine-level Settings enforcement (manual / share / watcher / provider runs bypass WorkManager constraints)
        if (!checkTransferConditions(settings).allowed) {
            return 0 to 0
        }

        val accessToken = try {
            authManager.getValidAccessToken()
        } catch (e: AuthTokenExpiredException) {
            _lastNetworkError.value = e.message
            return 0 to 0
        }

        // Check Drive Storage Quota before uploading
        try {
            val quota = driveApiClient.fetchDriveQuota(accessToken)
            _latestQuotaInfo.value = quota
            if (quota.isQuotaFull) {
                _quotaFullState.value = true
                _lastNetworkError.value = "Google Drive storage is full. Uploads paused safely without data loss."
                return 0 to 0
            }
            _quotaFullState.value = false
        } catch (e: StorageQuotaFullException) {
            _quotaFullState.value = true
            _lastNetworkError.value = e.message
            return 0 to 0
        } catch (_: Throwable) {
            // Proceed to upload if quota endpoint is temporarily unreachable
        }

        // Ensure active root backup folder exists on Google Drive
        val accountId = currentAccountId()
        val activeRootName = activeRootFolderName()
        val rootFolderId = try {
            val cachedRoot = authManager.accountState.value.rootDriveFolderId
            if (!cachedRoot.isNullOrBlank()) {
                reconcileForeignRootRecordsForAccount(accountId, cachedRoot)
                cachedRoot
            } else {
                val createdId = driveApiClient.ensureFolderExists(
                    accessToken = accessToken,
                    folderName = activeRootName,
                    parentFolderId = null
                )
                reconcileForeignRootRecordsForAccount(accountId, createdId)
                authManager.setRootDriveFolderName(activeRootName, createdId)
                createdId
            }
        } catch (e: Exception) {
            _lastNetworkError.value = "Failed to verify root folder '$activeRootName': ${e.message}"
            return 0 to 0
        }

        val concurrency = settings.uploadConcurrency.coerceIn(1, 4)

        // Every record is attempted at most once per pass. Paging skips records that are paused, still inside
        // their exponential retry backoff window, or already attempted, so they can never starve records behind
        // them in the queue (previously the first 50 blocked records stalled the whole queue).
        val attemptedIds = HashSet<Long>()
        var pageOffset = 0
        var batchesProcessed = 0

        while (!_isPaused.value) {
            val batch = backupDao.getPendingRecordsForAccountPaged(
                accountId = accountId,
                limit = UPLOAD_BATCH_SIZE,
                offset = pageOffset
            )
            if (batch.isEmpty()) break

            // Re-check Wi-Fi / charging between batches (e.g. the phone left Wi-Fi mid-run)
            if (batchesProcessed > 0 && !checkTransferConditions(settings).allowed) break
            batchesProcessed++

            val nowMs = System.currentTimeMillis()
            val eligibleRecords = ArrayList<BackupFileEntity>(batch.size)
            for (record in batch) {
                if (record.id in attemptedIds || record.id in pausedRecordIds) continue
                val job = backupDao.getSyncJobByRecordId(record.id)
                if (job?.isPaused == true) {
                    // Persisted per-file pause survives process death
                    pausedRecordIds.add(record.id)
                    continue
                }
                if (respectRetryBackoff) {
                    val nextRetryAt = job?.nextRetryAtMs
                    if (nextRetryAt != null && nextRetryAt > nowMs) continue
                }
                eligibleRecords.add(record)
            }

            if (eligibleRecords.isEmpty()) {
                pageOffset += batch.size
                continue
            }
            eligibleRecords.forEach { attemptedIds.add(it.id) }

            var verifiedInBatch = 0
            if (concurrency <= 1) {
                for (record in eligibleRecords) {
                    if (_isPaused.value) break
                    if (record.id in pausedRecordIds) continue

                    val success = uploadSingleRecordResumable(record, rootFolderId, settings)
                    if (success) {
                        verifiedCount++
                        verifiedInBatch++
                    } else if (!_isPaused.value && record.id !in pausedRecordIds) {
                        failedCount++
                        if (_quotaFullState.value) {
                            return verifiedCount to failedCount
                        }
                    }
                }
            } else {
                val semaphore = Semaphore(concurrency)
                val batchVerified = AtomicInteger(0)
                val batchFailed = AtomicInteger(0)

                coroutineScope {
                    eligibleRecords.map { record ->
                        async {
                            semaphore.withPermit {
                                if (_isPaused.value || _quotaFullState.value || record.id in pausedRecordIds) {
                                    return@withPermit
                                }
                                val success = uploadSingleRecordResumable(record, rootFolderId, settings)
                                if (success) {
                                    batchVerified.incrementAndGet()
                                } else if (!_isPaused.value && record.id !in pausedRecordIds) {
                                    batchFailed.incrementAndGet()
                                }
                            }
                        }
                    }.awaitAll()
                }
                verifiedInBatch = batchVerified.get()
                verifiedCount += verifiedInBatch
                failedCount += batchFailed.get()
            }
            if (_quotaFullState.value) break

            // VERIFIED records leave the pending set; everything else in this page stays in place.
            pageOffset += (batch.size - verifiedInBatch).coerceAtLeast(0)
        }

        return verifiedCount to failedCount
    }

    /**
     * Streams a single file record to Google Drive using Resumable Upload & verifies remote integrity.
     * Follows SyncJobState: PREPARING -> UPLOADING -> VERIFYING -> COMPLETED.
     * Each concurrent worker publishes its own entry in [activeUploads]; the entry is always removed when
     * the worker finishes (success, failure, pause or cancellation).
     */
    suspend fun uploadSingleRecordResumable(
        initialRecord: BackupFileEntity,
        rootDriveFolderId: String,
        settings: BackupSettings
    ): Boolean {
        return try {
            uploadSingleRecordResumableInternal(initialRecord, rootDriveFolderId, settings)
        } finally {
            clearUploadProgress(initialRecord.id)
        }
    }

    /**
     * Google Drive Desktop-style content deduplication: if identical bytes (same SHA-256 + size) were already
     * VERIFIED for this account, create an independent server-side copy instead of re-uploading.
     * The source Drive file is never modified, moved or deleted. Returns the new Drive file ID only when the
     * copy passes remote size/checksum verification; otherwise returns null and the caller uploads normally.
     */
    private suspend fun tryServerSideCopyDedup(
        accessToken: String,
        accountId: String,
        record: BackupFileEntity,
        targetFolderId: String
    ): String? {
        val sha256 = record.sha256Hash
        if (sha256.isNullOrBlank() || record.fileSize <= 0L) return null
        val duplicate = backupDao.findVerifiedDuplicateBySha256(
            accountId = accountId,
            sha256 = sha256,
            fileSize = record.fileSize,
            excludeRecordId = record.id
        ) ?: return null
        val sourceDriveFileId = duplicate.driveFileId ?: return null
        return try {
            val copied = driveApiClient.copyDriveFile(
                accessToken = accessToken,
                sourceDriveFileId = sourceDriveFileId,
                parentFolderId = targetFolderId,
                newName = record.driveStoredName,
                localSha256 = sha256,
                backupVersion = record.backupVersion
            )
            val verification = driveApiClient.verifyRemoteFile(
                accessToken = accessToken,
                driveFileId = copied.id,
                expectedSize = record.fileSize,
                expectedMd5 = record.md5Hash,
                expectedSha256 = sha256
            )
            if (verification == VerificationStatus.VERIFICATION_FAILED) null else copied.id
        } catch (e: StorageQuotaFullException) {
            throw e
        } catch (e: AuthTokenExpiredException) {
            throw e
        } catch (e: Exception) {
            if (e is CancellationException) throw e
            // Source missing on Drive (removed via web UI), permission or transient error -> regular upload
            null
        }
    }

    private suspend fun uploadSingleRecordResumableInternal(
        initialRecord: BackupFileEntity,
        rootDriveFolderId: String,
        settings: BackupSettings
    ): Boolean {
        val accountId = currentAccountId()
        val activeRootName = activeRootFolderName()
        val driveParentSubPath = initialRecord.relativePath.substringBeforeLast('/', "")
        val driveFullTarget = if (driveParentSubPath.isBlank()) {
            "My Drive/$activeRootName/${initialRecord.driveStoredName}"
        } else {
            "My Drive/$activeRootName/$driveParentSubPath/${initialRecord.driveStoredName}"
        }
        _currentScanActivity.value =
            "Backing up Phone: /storage/emulated/0/${initialRecord.relativePath} → Google Drive: $driveFullTarget"

        // Transition to PREPARING
        backupDao.transitionSyncJobState(
            accountId = accountId,
            recordId = initialRecord.id,
            sourceFileIdentity = initialRecord.sourceFileIdentity,
            fileName = initialRecord.driveStoredName,
            relativePath = initialRecord.relativePath,
            backupVersion = initialRecord.backupVersion,
            newState = SyncJobState.PREPARING,
            uploadedBytes = initialRecord.uploadedBytes,
            totalBytes = initialRecord.fileSize
        )

        // Verify source file still exists on phone before uploading
        if (!storageSource.exists(initialRecord.sourceUri)) {
            val detail = "Source file was deleted from phone before initial upload completed; cannot recover unuploaded file."
            backupDao.markUploadFailed(
                id = initialRecord.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.SOURCE_DELETED_BEFORE_UPLOAD,
                detail = detail,
                sessionUri = null
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = initialRecord,
                reason = FailureReason.SOURCE_DELETED_BEFORE_UPLOAD,
                detail = detail,
                retryable = false,
                sessionUri = null
            )
            return false
        }

        // Compute streaming SHA-256 and MD5 lazily if not yet computed (e.g. for files > 256 KiB queued with fast scan)
        val record = if (initialRecord.sha256Hash == null || initialRecord.md5Hash == null) {
            try {
                val hashes = SourceFileReadOnlyGuard.computeStreamingHashes {
                    storageSource.openInputStream(initialRecord.sourceUri)
                }
                val updated = initialRecord.copy(
                    sha256Hash = hashes.sha256Hex,
                    md5Hash = hashes.md5Hex
                )
                backupDao.updateRecord(updated)
                backupDao.getLocalFileByIdentity(updated.sourceFileIdentity)?.let { local ->
                    backupDao.upsertLocalFile(
                        local.copy(
                            latestSha256 = hashes.sha256Hex,
                            latestMd5 = hashes.md5Hex
                        )
                    )
                }
                updated
            } catch (e: Exception) {
                val detail = "Unable to read source stream: ${e.message}"
                backupDao.markUploadFailed(
                    id = initialRecord.id,
                    status = UploadStatus.FAILED,
                    reason = FailureReason.PERMISSION_REVOKED,
                    detail = detail,
                    sessionUri = initialRecord.resumableSessionUri
                )
                recordFailureInDatabase(
                    accountId = accountId,
                    record = initialRecord,
                    reason = FailureReason.PERMISSION_REVOKED,
                    detail = detail,
                    retryable = false,
                    sessionUri = initialRecord.resumableSessionUri
                )
                return false
            }
        } else {
            initialRecord
        }

        val accessToken = try {
            authManager.getValidAccessToken()
        } catch (e: AuthTokenExpiredException) {
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.AUTH_TOKEN_EXPIRED,
                detail = e.message,
                sessionUri = record.resumableSessionUri
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.AUTH_TOKEN_EXPIRED,
                detail = e.message ?: "OAuth token expired",
                retryable = false,
                sessionUri = record.resumableSessionUri
            )
            return false
        }

        val chunkSize = settings.chunkSizeBytes.coerceAtLeast(DriveApiClient.DEFAULT_CHUNK_SIZE_BYTES)

        try {
            // Direct uploads into an explicit Drive folder (My Drive "Upload File") bypass the mirrored hierarchy
            val targetFolderId = explicitDriveTargetFolderId(record.sourceFileIdentity)
                ?: resolveDriveSubFolderHierarchy(
                    accessToken = accessToken,
                    rootFolderId = rootDriveFolderId,
                    relativeFilePath = record.relativePath
                )

            // Section 7 Reconciliation Check: If a previous upload succeeded remotely right before a local crash
            // or DB failure, reconcile with the existing remote file without uploading a duplicate!
            if (record.retryCount > 0 || !record.resumableSessionUri.isNullOrBlank()) {
                val existingRemote = driveApiClient.findExistingBackedUpFile(
                    accessToken = accessToken,
                    parentFolderId = targetFolderId,
                    driveFileName = record.driveStoredName,
                    expectedSize = record.fileSize,
                    expectedSha256 = record.sha256Hash
                )
                if (existingRemote != null && existingRemote.id.isNotBlank()) {
                    return finalizeAndVerifyUpload(
                        accessToken = accessToken,
                        record = record,
                        driveFileId = existingRemote.id,
                        driveFolderId = targetFolderId
                    )
                }
            }

            var sessionUri = record.resumableSessionUri
            var resumeOffset = 0L

            // Step A: If an existing resumable session URI exists in DB, probe Google Drive for exact byte offset
            if (!sessionUri.isNullOrBlank()) {
                try {
                    val probe = driveApiClient.queryResumableUploadOffset(sessionUri, record.fileSize)
                    if (probe.completed && !probe.driveFileId.isNullOrBlank()) {
                        return finalizeAndVerifyUpload(
                            accessToken = accessToken,
                            record = record,
                            driveFileId = probe.driveFileId,
                            driveFolderId = targetFolderId
                        )
                    }
                    resumeOffset = probe.confirmedUploadedBytes
                } catch (_: UploadSessionExpiredException) {
                    sessionUri = null
                    resumeOffset = 0L
                }
            }

            // Step B: Start a new Resumable Upload session if needed
            if (sessionUri.isNullOrBlank()) {
                // Step B0: Content dedup (moved / renamed / duplicated files) via server-side copy — zero re-upload
                val dedupDriveFileId = tryServerSideCopyDedup(
                    accessToken = accessToken,
                    accountId = accountId,
                    record = record,
                    targetFolderId = targetFolderId
                )
                if (dedupDriveFileId != null) {
                    return finalizeAndVerifyUpload(
                        accessToken = accessToken,
                        record = record,
                        driveFileId = dedupDriveFileId,
                        driveFolderId = targetFolderId
                    )
                }

                sessionUri = driveApiClient.initiateResumableUploadSession(
                    accessToken = accessToken,
                    parentFolderId = targetFolderId,
                    driveFileName = record.driveStoredName,
                    mimeType = record.mimeType,
                    totalBytes = record.fileSize,
                    localSha256 = record.sha256Hash,
                    backupVersion = record.backupVersion
                )
                resumeOffset = 0L
                backupDao.updateUploadProgress(
                    id = record.id,
                    uploadedBytes = 0L,
                    sessionUri = sessionUri,
                    status = UploadStatus.IN_PROGRESS
                )
            }

            backupDao.upsertUploadSession(
                UploadSessionEntity(
                    sessionId = "${accountId}_${record.id}_v${record.backupVersion}",
                    accountId = accountId,
                    recordId = record.id,
                    sourceFileIdentity = record.sourceFileIdentity,
                    backupVersion = record.backupVersion,
                    resumableSessionUri = sessionUri,
                    uploadedBytes = resumeOffset,
                    totalBytes = record.fileSize,
                    chunkSizeBytes = chunkSize,
                    isActive = true
                )
            )

            backupDao.transitionSyncJobState(
                accountId = accountId,
                recordId = record.id,
                sourceFileIdentity = record.sourceFileIdentity,
                fileName = record.driveStoredName,
                relativePath = record.relativePath,
                backupVersion = record.backupVersion,
                newState = SyncJobState.UPLOADING,
                uploadedBytes = resumeOffset,
                totalBytes = record.fileSize
            )

            // Step C: Stream chunks from resumeOffset using bounded 64 KiB buffer (Zero temp copy!)
            storageSource.openInputStream(record.sourceUri).use { rawStream ->
                if (resumeOffset > 0L) {
                    SourceFileReadOnlyGuard.skipStreamToOffset(rawStream, resumeOffset)
                }

                var currentOffset = resumeOffset
                var completedDriveFileId: String? = null
                val uploadStartTimeMs = System.currentTimeMillis()
                val initialOffsetAtStart = resumeOffset

                if (record.fileSize == 0L) {
                    val emptyResult = driveApiClient.uploadSingleStreamChunk(
                        sessionUri = sessionUri,
                        inputStream = rawStream,
                        chunkStartByte = 0L,
                        chunkLength = 0,
                        totalBytes = 0L,
                        mimeType = record.mimeType,
                        maxUploadSpeedKbps = settings.maxUploadSpeedKbps
                    )
                    completedDriveFileId = emptyResult.driveFileId
                } else {
                    while (currentOffset < record.fileSize) {
                        if (_isPaused.value || record.id in pausedRecordIds) {
                            backupDao.updateUploadProgress(
                                id = record.id,
                                uploadedBytes = currentOffset,
                                sessionUri = sessionUri,
                                status = UploadStatus.PENDING_RETRY
                            )
                            backupDao.transitionSyncJobState(
                                accountId = accountId,
                                recordId = record.id,
                                sourceFileIdentity = record.sourceFileIdentity,
                                fileName = record.driveStoredName,
                                relativePath = record.relativePath,
                                backupVersion = record.backupVersion,
                                newState = SyncJobState.RETRY_SCHEDULED,
                                uploadedBytes = currentOffset,
                                totalBytes = record.fileSize,
                                errorMessage = "Upload paused by user at byte offset $currentOffset"
                            )
                            return false
                        }

                        val elapsedMs = (System.currentTimeMillis() - uploadStartTimeMs).coerceAtLeast(1L)
                        val bytesTransferredThisRun = (currentOffset - initialOffsetAtStart).coerceAtLeast(0L)
                        val speedBps = if (bytesTransferredThisRun > 0L) {
                            (bytesTransferredThisRun * 1000L) / elapsedMs
                        } else {
                            0L
                        }
                        val remainingBytes = (record.fileSize - currentOffset).coerceAtLeast(0L)
                        val etaSeconds = if (speedBps > 0L) remainingBytes / speedBps else null

                        publishUploadProgress(
                            ActiveUploadProgress(
                                recordId = record.id,
                                fileName = record.driveStoredName,
                                relativePath = record.relativePath,
                                backupVersion = record.backupVersion,
                                uploadedBytes = currentOffset,
                                totalBytes = record.fileSize,
                                resumableSessionActive = true,
                                transferSpeedBps = speedBps,
                                estimatedRemainingSeconds = etaSeconds,
                                syncJobState = SyncJobState.UPLOADING,
                                isPaused = record.id in pausedRecordIds
                            )
                        )

                        val nextChunkLen = minOf(chunkSize.toLong(), remainingBytes).toInt()

                        val chunkResult = driveApiClient.uploadSingleStreamChunk(
                            sessionUri = sessionUri,
                            inputStream = rawStream,
                            chunkStartByte = currentOffset,
                            chunkLength = nextChunkLen,
                            totalBytes = record.fileSize,
                            mimeType = record.mimeType,
                            maxUploadSpeedKbps = settings.maxUploadSpeedKbps
                        )

                        currentOffset = chunkResult.confirmedUploadedBytes
                        backupDao.updateUploadProgress(
                            id = record.id,
                            uploadedBytes = currentOffset,
                            sessionUri = sessionUri,
                            status = UploadStatus.IN_PROGRESS
                        )
                        backupDao.upsertUploadSession(
                            UploadSessionEntity(
                                sessionId = "${accountId}_${record.id}_v${record.backupVersion}",
                                accountId = accountId,
                                recordId = record.id,
                                sourceFileIdentity = record.sourceFileIdentity,
                                backupVersion = record.backupVersion,
                                resumableSessionUri = sessionUri,
                                uploadedBytes = currentOffset,
                                totalBytes = record.fileSize,
                                chunkSizeBytes = chunkSize,
                                isActive = !chunkResult.completed
                            )
                        )

                        if (chunkResult.completed) {
                            completedDriveFileId = chunkResult.driveFileId
                            break
                        }
                    }
                }

                if (completedDriveFileId.isNullOrBlank()) {
                    throw IOException("Resumable upload finished streaming bytes without returning a Drive file ID")
                }

                return finalizeAndVerifyUpload(
                    accessToken = accessToken,
                    record = record,
                    driveFileId = completedDriveFileId,
                    driveFolderId = targetFolderId
                )
            }
        } catch (e: UploadSessionExpiredException) {
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.SESSION_EXPIRED_RESTARTED,
                detail = e.message,
                sessionUri = null
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.SESSION_EXPIRED_RESTARTED,
                detail = e.message ?: "Upload session expired",
                retryable = true,
                sessionUri = null
            )
            return false
        } catch (e: DriveResourceNotFoundException) {
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.SESSION_EXPIRED_RESTARTED,
                detail = e.message,
                sessionUri = null
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.SESSION_EXPIRED_RESTARTED,
                detail = e.message ?: "HTTP 404 Resource Not Found",
                retryable = true,
                sessionUri = null,
                httpStatusCode = 404
            )
            return false
        } catch (e: DriveConflictException) {
            val latestSaved = backupDao.getRecordById(record.id)
            val savedSession = latestSaved?.resumableSessionUri ?: record.resumableSessionUri
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message,
                sessionUri = savedSession
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = latestSaved ?: record,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message ?: "HTTP 409 Conflict",
                retryable = true,
                sessionUri = savedSession,
                httpStatusCode = 409
            )
            return false
        } catch (e: DriveServerException) {
            val latestSaved = backupDao.getRecordById(record.id)
            val savedSession = latestSaved?.resumableSessionUri ?: record.resumableSessionUri
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message,
                sessionUri = savedSession
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = latestSaved ?: record,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message ?: "HTTP ${e.httpStatusCode} Server Error",
                retryable = true,
                sessionUri = savedSession,
                httpStatusCode = e.httpStatusCode
            )
            return false
        } catch (e: StorageQuotaFullException) {
            _quotaFullState.value = true
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.STORAGE_QUOTA_FULL,
                detail = e.message,
                sessionUri = record.resumableSessionUri
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.STORAGE_QUOTA_FULL,
                detail = e.message ?: "Google Drive quota full",
                retryable = false,
                sessionUri = record.resumableSessionUri,
                httpStatusCode = 403
            )
            return false
        } catch (e: DrivePermissionException) {
            _lastPermissionError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.PERMISSION_REVOKED,
                detail = e.message,
                sessionUri = record.resumableSessionUri
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.PERMISSION_REVOKED,
                detail = e.message ?: "Drive permission denied",
                retryable = false,
                sessionUri = record.resumableSessionUri,
                httpStatusCode = 403
            )
            return false
        } catch (e: DriveBadRequestException) {
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.BAD_REQUEST,
                detail = e.message,
                sessionUri = null
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.BAD_REQUEST,
                detail = e.message ?: "HTTP 400 Bad Request",
                retryable = false,
                sessionUri = null,
                httpStatusCode = 400
            )
            return false
        } catch (e: DriveRateLimitException) {
            val latestSaved = backupDao.getRecordById(record.id)
            val savedSession = latestSaved?.resumableSessionUri ?: record.resumableSessionUri
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.RATE_LIMITED,
                detail = e.message,
                sessionUri = savedSession
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = latestSaved ?: record,
                reason = FailureReason.RATE_LIMITED,
                detail = e.message ?: "Rate limit reached",
                retryable = true,
                sessionUri = savedSession,
                httpStatusCode = 429
            )
            return false
        } catch (e: AuthTokenExpiredException) {
            _lastNetworkError.value = e.message
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.AUTH_TOKEN_EXPIRED,
                detail = e.message,
                sessionUri = record.resumableSessionUri
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.AUTH_TOKEN_EXPIRED,
                detail = e.message ?: "OAuth token expired",
                retryable = true,
                sessionUri = record.resumableSessionUri,
                httpStatusCode = 401
            )
            return false
        } catch (e: IOException) {
            val latestSaved = backupDao.getRecordById(record.id)
            val savedSession = latestSaved?.resumableSessionUri ?: record.resumableSessionUri
            _lastNetworkError.value = "Network interruption during streaming upload: ${e.message}"
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message,
                sessionUri = savedSession
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = latestSaved ?: record,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message ?: "Network interruption",
                retryable = true,
                sessionUri = savedSession
            )
            return false
        }
    }

    private suspend fun recordFailureInDatabase(
        accountId: String,
        record: BackupFileEntity,
        reason: FailureReason,
        detail: String,
        retryable: Boolean,
        sessionUri: String?,
        httpStatusCode: Int? = null
    ) {
        val targetState = if (retryable) SyncJobState.FAILED_RETRYABLE else SyncJobState.FAILED_ACTION_REQUIRED
        backupDao.transitionSyncJobState(
            accountId = accountId,
            recordId = record.id,
            sourceFileIdentity = record.sourceFileIdentity,
            fileName = record.driveStoredName,
            relativePath = record.relativePath,
            backupVersion = record.backupVersion,
            newState = targetState,
            uploadedBytes = record.uploadedBytes,
            totalBytes = record.fileSize,
            errorMessage = detail
        )
        if (retryable) {
            backupDao.transitionSyncJobState(
                accountId = accountId,
                recordId = record.id,
                sourceFileIdentity = record.sourceFileIdentity,
                fileName = record.driveStoredName,
                relativePath = record.relativePath,
                backupVersion = record.backupVersion,
                newState = SyncJobState.RETRY_SCHEDULED,
                uploadedBytes = record.uploadedBytes,
                totalBytes = record.fileSize,
                errorMessage = detail
            )
        }
        backupDao.insertFailedOperation(
            FailedOperationEntity(
                accountId = accountId,
                recordId = record.id,
                sourceFileIdentity = record.sourceFileIdentity,
                operationType = "RESUMABLE_UPLOAD",
                failureReason = reason,
                httpStatusCode = httpStatusCode,
                errorDetail = detail,
                resumableSessionUri = sessionUri,
                uploadedBytes = record.uploadedBytes,
                retryable = retryable,
                retryCount = record.retryCount + 1
            )
        )
    }

    private suspend fun finalizeAndVerifyUpload(
        accessToken: String,
        record: BackupFileEntity,
        driveFileId: String,
        driveFolderId: String
    ): Boolean {
        val accountId = currentAccountId()
        backupDao.transitionSyncJobState(
            accountId = accountId,
            recordId = record.id,
            sourceFileIdentity = record.sourceFileIdentity,
            fileName = record.driveStoredName,
            relativePath = record.relativePath,
            backupVersion = record.backupVersion,
            newState = SyncJobState.VERIFYING,
            uploadedBytes = record.fileSize,
            totalBytes = record.fileSize
        )

        val verification = driveApiClient.verifyRemoteFile(
            accessToken = accessToken,
            driveFileId = driveFileId,
            expectedSize = record.fileSize,
            expectedMd5 = record.md5Hash,
            expectedSha256 = record.sha256Hash
        )

        if (verification == VerificationStatus.VERIFICATION_FAILED) {
            val detail = "Remote Drive file size or checksum did not match local source file"
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.VERIFICATION_MISMATCH,
                detail = detail,
                sessionUri = null
            )
            recordFailureInDatabase(
                accountId = accountId,
                record = record,
                reason = FailureReason.VERIFICATION_MISMATCH,
                detail = detail,
                retryable = true,
                sessionUri = null
            )
            return false
        }

        val now = System.currentTimeMillis()
        backupDao.markUploadVerified(
            id = record.id,
            driveFileId = driveFileId,
            driveFolderId = driveFolderId,
            fileSize = record.fileSize,
            verificationStatus = verification,
            completedAtMs = now
        )
        backupDao.deactivateUploadSession("${accountId}_${record.id}_v${record.backupVersion}", now)
        backupDao.markFailuresResolvedForRecord(record.id)

        // Ensure LocalFileEntity exists before inserting BackupVersionEntity (ForeignKey constraint)
        backupDao.upsertLocalFile(
            LocalFileEntity(
                sourceFileIdentity = record.sourceFileIdentity,
                sourceUri = record.sourceUri,
                relativePath = record.relativePath,
                displayName = record.displayName,
                folderCategory = record.folderCategory,
                fileSize = record.fileSize,
                lastModifiedMs = record.lastModifiedMs,
                mimeType = record.mimeType,
                latestSha256 = record.sha256Hash,
                latestMd5 = record.md5Hash,
                latestVersion = record.backupVersion,
                localExists = record.localExists,
                lastSeenMs = now
            )
        )

        // Record immutable BackupVersionEntity
        backupDao.insertBackupVersion(
            BackupVersionEntity(
                accountId = accountId,
                sourceFileIdentity = record.sourceFileIdentity,
                versionNumber = record.backupVersion,
                driveFileId = driveFileId,
                driveFolderId = driveFolderId,
                driveStoredName = record.driveStoredName,
                relativePath = record.relativePath,
                fileSize = record.fileSize,
                sha256Hash = record.sha256Hash,
                md5Hash = record.md5Hash,
                verificationStatus = verification,
                preservedAfterLocalDelete = !record.localExists,
                createdAtMs = now
            )
        )

        // Index in CloudFileEntity for My Drive Explorer & Version Restore
        backupDao.upsertCloudFile(
            CloudFileEntity(
                driveFileId = driveFileId,
                accountId = accountId,
                name = record.driveStoredName,
                mimeType = record.mimeType,
                sizeBytes = record.fileSize,
                md5Checksum = record.md5Hash,
                sha256Checksum = record.sha256Hash,
                parentFolderId = driveFolderId,
                relativePath = record.relativePath,
                isFolder = false,
                isProtectedBackupFile = true,
                createdTimeMs = now,
                modifiedTimeMs = record.lastModifiedMs,
                lastSyncedMs = now
            )
        )

        backupDao.transitionSyncJobState(
            accountId = accountId,
            recordId = record.id,
            sourceFileIdentity = record.sourceFileIdentity,
            fileName = record.driveStoredName,
            relativePath = record.relativePath,
            backupVersion = record.backupVersion,
            newState = SyncJobState.COMPLETED,
            uploadedBytes = record.fileSize,
            totalBytes = record.fileSize,
            transferSpeedBps = _activeUploads.value[record.id]?.transferSpeedBps ?: 0L
        )

        _lastNetworkError.value = null
        val activeRootName = activeRootFolderName()
        val verifiedParentSubPath = record.relativePath.substringBeforeLast('/', "")
        val verifiedDriveTarget = if (verifiedParentSubPath.isBlank()) {
            "My Drive/$activeRootName/${record.driveStoredName}"
        } else {
            "My Drive/$activeRootName/$verifiedParentSubPath/${record.driveStoredName}"
        }
        _currentScanActivity.value =
            "✓ Backed up Phone: /storage/emulated/0/${record.relativePath} → Google Drive: $verifiedDriveTarget"

        // Post-verification hooks (e.g. delete app-private share staging copy). Failures never undo VERIFIED.
        try {
            onRecordVerified?.invoke(
                record.copy(
                    uploadStatus = UploadStatus.VERIFIED,
                    driveFileId = driveFileId,
                    driveFolderId = driveFolderId
                )
            )
        } catch (e: Exception) {
            if (e is CancellationException) throw e
        }
        return true
    }

    /**
     * Preserves the phone's folder hierarchy inside the active Google Drive backup root folder
     * (e.g. "DCIM/Camera/IMG_001.jpg" -> creates/reuses "<rootDriveFolderName>/DCIM/Camera").
     * Uses account-scoped (and custom root-scoped) cache key so switching accounts or renaming
     * the root backup folder never mixes folder IDs!
     */
    private suspend fun resolveDriveSubFolderHierarchy(
        accessToken: String,
        rootFolderId: String,
        relativeFilePath: String
    ): String {
        val normalized = relativeFilePath.replace('\\', '/').trim('/')
        val parentPath = normalized.substringBeforeLast('/', "")
        if (parentPath.isBlank()) return rootFolderId

        return folderCreationMutex.withLock {
            val accountPrefix = currentAccountId()
            val rootScope = if (rootFolderId.isNotBlank()) {
                "$accountPrefix:$rootFolderId"
            } else {
                accountPrefix
            }
            val scopedParentKey = "$rootScope:$parentPath"
            backupDao.getCachedDriveFolder(scopedParentKey)?.let { return@withLock it.driveFolderId }

            val segments = parentPath.split('/').filter { it.isNotBlank() }
            var currentParentId = rootFolderId
            val pathBuilder = StringBuilder()

            for (segment in segments) {
                if (pathBuilder.isNotEmpty()) pathBuilder.append('/')
                pathBuilder.append(segment)
                val partialPath = pathBuilder.toString()
                val scopedKey = "$rootScope:$partialPath"

                val cached = backupDao.getCachedDriveFolder(scopedKey)
                if (cached != null) {
                    currentParentId = cached.driveFolderId
                } else {
                    val folderId = driveApiClient.ensureFolderExists(
                        accessToken = accessToken,
                        folderName = segment,
                        parentFolderId = currentParentId
                    )
                    val entity = DriveFolderCacheEntity(
                        relativePath = scopedKey,
                        driveFolderId = folderId,
                        parentDriveFolderId = currentParentId
                    )
                    backupDao.cacheDriveFolder(entity)
                    // Also cache unscoped relativePath for backward-compatible test assertions
                    backupDao.cacheDriveFolder(
                        DriveFolderCacheEntity(
                            relativePath = partialPath,
                            driveFolderId = folderId,
                            parentDriveFolderId = currentParentId
                        )
                    )
                    currentParentId = folderId
                }
            }

            currentParentId
        }
    }

    private fun currentAccountId(): String {
        return authManager.accountState.value.accountId.ifBlank {
            authManager.accountState.value.email.trim().lowercase().ifBlank { "default_account" }
        }
    }
}
