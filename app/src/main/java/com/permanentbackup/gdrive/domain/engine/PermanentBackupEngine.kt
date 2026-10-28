package com.permanentbackup.gdrive.domain.engine

import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.DriveFolderCacheEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.model.ActiveUploadProgress
import com.permanentbackup.gdrive.domain.model.AuthTokenExpiredException
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.DiscoveredSourceFile
import com.permanentbackup.gdrive.domain.model.DriveQuotaInfo
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.StorageQuotaFullException
import com.permanentbackup.gdrive.domain.model.UploadSessionExpiredException
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.io.IOException

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
 * Core Permanent Backup Engine enforcing:
 * 1. Strictly Upload-Only, Append-Only semantics (Never deletes or overwrites remote Drive files).
 * 2. Versioned preservation when a local file is modified, moved, or renamed.
 * 3. Streaming Resumable Chunked Upload with exact byte-offset recovery on network failure.
 * 4. Post-upload Remote Metadata, File Size & SHA-256/MD5 Checksum verification.
 * 5. Paged database processing supporting 100,000+ files with bounded memory.
 */
class PermanentBackupEngine(
    private val backupDao: BackupDao,
    private val storageSource: StorageFileSource,
    private val driveApiClient: DriveApiClient,
    private val authManager: GoogleAuthManager
) {
    companion object {
        const val ROOT_DRIVE_FOLDER_NAME = "Android_Permanent_Backup"
        const val PAGINATION_BATCH_SIZE = 500
    }

    private val _activeUploadProgress = MutableStateFlow<ActiveUploadProgress?>(null)
    val activeUploadProgress: StateFlow<ActiveUploadProgress?> = _activeUploadProgress.asStateFlow()

    private val _isRunning = MutableStateFlow(false)
    val isRunning: StateFlow<Boolean> = _isRunning.asStateFlow()

    private val _lastPermissionError = MutableStateFlow<String?>(null)
    val lastPermissionError: StateFlow<String?> = _lastPermissionError.asStateFlow()

    private val _lastNetworkError = MutableStateFlow<String?>(null)
    val lastNetworkError: StateFlow<String?> = _lastNetworkError.asStateFlow()

    private val _quotaFullState = MutableStateFlow(false)
    val quotaFullState: StateFlow<Boolean> = _quotaFullState.asStateFlow()

    private val _latestQuotaInfo = MutableStateFlow(DriveQuotaInfo())
    val latestQuotaInfo: StateFlow<DriveQuotaInfo> = _latestQuotaInfo.asStateFlow()

    /**
     * Recovers any IN_PROGRESS tasks left incomplete due to an app crash or device reboot,
     * preserving their resumableSessionUri and uploadedBytes progress.
     */
    suspend fun recoverInterruptedUploadsAfterRestart(): Int {
        val interrupted = backupDao.getInProgressRecords()
        for (record in interrupted) {
            backupDao.updateUploadProgress(
                id = record.id,
                uploadedBytes = record.uploadedBytes,
                sessionUri = record.resumableSessionUri,
                status = UploadStatus.PENDING_RETRY
            )
        }
        return interrupted.size
    }

    /**
     * Executes a complete scan + deduplication + version detection + resumable upload cycle.
     */
    suspend fun runBackupCycle(settings: BackupSettings): BackupCycleSummary {
        if (!settings.userBackupConsentGranted) {
            val msg = "User backup consent not granted yet. No files were scanned or read."
            _lastPermissionError.value = msg
            return BackupCycleSummary(0, 0, 0, 0, 0, 0, permissionDenied = true, errorMessage = msg)
        }

        if (!storageSource.hasStoragePermission) {
            val msg = "Storage access permission was revoked or denied. Please grant storage access."
            _lastPermissionError.value = msg
            return BackupCycleSummary(0, 0, 0, 0, 0, 0, permissionDenied = true, errorMessage = msg)
        }

        _lastPermissionError.value = null
        _isRunning.value = true

        try {
            // Step 1: Recover any interrupted tasks from previous crash/reboot
            recoverInterruptedUploadsAfterRestart()

            // Step 2: Scan accessible folders in streaming sequence & evaluate deduplication/versioning
            var scannedCount = 0
            var newlyQueued = 0
            var skippedUnchanged = 0
            val discoveredIdentities = HashSet<String>(4096)

            for (discovered in storageSource.scanFilesSequence(computeHashes = true)) {
                scannedCount++
                discoveredIdentities.add(discovered.sourceFileIdentity)
                val queued = evaluateAndQueueDiscoveredFile(discovered)
                if (queued) {
                    newlyQueued++
                } else {
                    skippedUnchanged++
                }
            }

            // Step 3: Reconcile locally deleted files — NEVER delete from Google Drive!
            val preservedDeletedCount = reconcileLocallyDeletedFiles(discoveredIdentities)

            // Step 4: Execute pending uploads if Google Account is connected
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

            val uploadOutcome = processPendingUploadQueue(settings)
            return BackupCycleSummary(
                scannedFiles = scannedCount,
                newlyQueuedVersions = newlyQueued,
                skippedUnchangedFiles = skippedUnchanged,
                uploadedAndVerifiedFiles = uploadOutcome.first,
                failedUploads = uploadOutcome.second,
                preservedDeletedLocalFiles = preservedDeletedCount,
                quotaFull = _quotaFullState.value,
                errorMessage = _lastNetworkError.value
            )
        } finally {
            _activeUploadProgress.value = null
            _isRunning.value = false
        }
    }

    /**
     * Evaluates a single discovered file against the Room database:
     * - Unchanged file -> skipped (returns false)
     * - New file -> queued as version 1 (returns true)
     * - Modified file -> previous verified version preserved untouched; new version (N+1) queued (returns true)
     */
    suspend fun evaluateAndQueueDiscoveredFile(discovered: DiscoveredSourceFile): Boolean {
        val latest = backupDao.getLatestVersionForIdentity(discovered.sourceFileIdentity)

        if (latest == null) {
            backupDao.insertRecord(
                BackupFileEntity(
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
            return true
        }

        // If the latest version is still pending/in-progress/retry for the exact same file state, keep it queued
        val isSameContent = isUnchangedFile(latest, discovered)
        if (isSameContent) {
            if (!latest.localExists) {
                backupDao.updateRecord(latest.copy(localExists = true))
            }
            return latest.uploadStatus != UploadStatus.VERIFIED
        }

        // If latest version was NOT yet verified (e.g. failed before finishing) and local file changed,
        // create a new version so history remains append-only and deterministic.
        val nextVersion = latest.backupVersion + 1
        val versionedDriveName = buildVersionedDriveFileName(
            originalName = discovered.displayName,
            version = nextVersion,
            sha256Hash = discovered.sha256Hash,
            lastModifiedMs = discovered.lastModifiedMs
        )

        backupDao.insertRecord(
            BackupFileEntity(
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
        var offset = 0
        val missingIdentities = mutableListOf<String>()
        while (true) {
            val page = backupDao.getActiveIdentitiesPaged(PAGINATION_BATCH_SIZE, offset)
            if (page.isEmpty()) break
            for (identity in page) {
                if (identity !in presentIdentities) {
                    missingIdentities.add(identity)
                }
            }
            offset += page.size
        }

        for (identity in missingIdentities) {
            backupDao.markLocalFileDeletedPreservingCloudBackup(identity)
        }
        return backupDao.countPreservedAfterLocalDelete()
    }

    private suspend fun processPendingUploadQueue(settings: BackupSettings): Pair<Int, Int> {
        var verifiedCount = 0
        var failedCount = 0

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

        // Ensure "Android_Permanent_Backup" root folder exists
        val rootFolderId = try {
            val cachedRoot = authManager.accountState.value.rootDriveFolderId
            if (!cachedRoot.isNullOrBlank()) {
                cachedRoot
            } else {
                val createdId = driveApiClient.ensureFolderExists(
                    accessToken = accessToken,
                    folderName = ROOT_DRIVE_FOLDER_NAME,
                    parentFolderId = null
                )
                authManager.setRootDriveFolderId(createdId)
                createdId
            }
        } catch (e: Exception) {
            _lastNetworkError.value = "Failed to verify root folder '$ROOT_DRIVE_FOLDER_NAME': ${e.message}"
            return 0 to 0
        }

        while (true) {
            val batch = backupDao.getPendingRecordsPaged(limit = 50, offset = 0)
            if (batch.isEmpty()) break

            var madeProgressInBatch = false
            for (record in batch) {
                val success = uploadSingleRecordResumable(record, rootFolderId, settings)
                if (success) {
                    verifiedCount++
                    madeProgressInBatch = true
                } else {
                    failedCount++
                    if (_quotaFullState.value) {
                        // Stop immediately if Google Drive storage quota is full
                        return verifiedCount to failedCount
                    }
                }
            }
            if (!madeProgressInBatch) break
        }

        return verifiedCount to failedCount
    }

    /**
     * Streams a single file record to Google Drive using Resumable Upload & verifies remote integrity.
     */
    suspend fun uploadSingleRecordResumable(
        initialRecord: BackupFileEntity,
        rootDriveFolderId: String,
        settings: BackupSettings
    ): Boolean {
        // Verify source file still exists on phone before uploading
        if (!storageSource.exists(initialRecord.sourceUri)) {
            backupDao.markUploadFailed(
                id = initialRecord.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.SOURCE_DELETED_BEFORE_UPLOAD,
                detail = "Source file was deleted from phone before initial upload completed; cannot recover unuploaded file.",
                sessionUri = null
            )
            return false
        }

        // Compute streaming SHA-256 and MD5 lazily if not yet computed (e.g. for multi-GB files)
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
                updated
            } catch (e: Exception) {
                backupDao.markUploadFailed(
                    id = initialRecord.id,
                    status = UploadStatus.FAILED,
                    reason = FailureReason.PERMISSION_REVOKED,
                    detail = "Unable to read source stream: ${e.message}",
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
            return false
        }

        val chunkSize = settings.chunkSizeBytes.coerceAtLeast(DriveApiClient.DEFAULT_CHUNK_SIZE_BYTES)

        try {
            val targetFolderId = resolveDriveSubFolderHierarchy(
                accessToken = accessToken,
                rootFolderId = rootDriveFolderId,
                relativeFilePath = record.relativePath
            )

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
                    // Rule 8.9: Expired upload session -> safely reset and start a fresh session
                    sessionUri = null
                    resumeOffset = 0L
                }
            }

            // Step B: Start a new Resumable Upload session if needed
            if (sessionUri.isNullOrBlank()) {
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

            // Step C: Stream chunks from resumeOffset using bounded 64 KiB buffer (Zero temp copy!)
            storageSource.openInputStream(record.sourceUri).use { rawStream ->
                if (resumeOffset > 0L) {
                    SourceFileReadOnlyGuard.skipStreamToOffset(rawStream, resumeOffset)
                }

                var currentOffset = resumeOffset
                var completedDriveFileId: String? = null

                if (record.fileSize == 0L) {
                    val emptyResult = driveApiClient.uploadSingleStreamChunk(
                        sessionUri = sessionUri!!,
                        inputStream = rawStream,
                        chunkStartByte = 0L,
                        chunkLength = 0,
                        totalBytes = 0L,
                        mimeType = record.mimeType
                    )
                    completedDriveFileId = emptyResult.driveFileId
                } else {
                    while (currentOffset < record.fileSize) {
                        _activeUploadProgress.value = ActiveUploadProgress(
                            recordId = record.id,
                            fileName = record.driveStoredName,
                            relativePath = record.relativePath,
                            backupVersion = record.backupVersion,
                            uploadedBytes = currentOffset,
                            totalBytes = record.fileSize,
                            resumableSessionActive = true
                        )

                        val remaining = record.fileSize - currentOffset
                        val nextChunkLen = minOf(chunkSize.toLong(), remaining).toInt()

                        val chunkResult = driveApiClient.uploadSingleStreamChunk(
                            sessionUri = sessionUri!!,
                            inputStream = rawStream,
                            chunkStartByte = currentOffset,
                            chunkLength = nextChunkLen,
                            totalBytes = record.fileSize,
                            mimeType = record.mimeType
                        )

                        currentOffset = chunkResult.confirmedUploadedBytes
                        backupDao.updateUploadProgress(
                            id = record.id,
                            uploadedBytes = currentOffset,
                            sessionUri = sessionUri,
                            status = UploadStatus.IN_PROGRESS
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
            // Clear expired session URI so next retry starts a fresh session cleanly
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.SESSION_EXPIRED_RESTARTED,
                detail = e.message,
                sessionUri = null
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
            return false
        } catch (e: IOException) {
            // Preserve current resumableSessionUri in DB so retry resumes from exact chunk!
            val latestSaved = backupDao.getRecordById(record.id)
            _lastNetworkError.value = "Network interruption during streaming upload: ${e.message}"
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.PENDING_RETRY,
                reason = FailureReason.NETWORK_ERROR,
                detail = e.message,
                sessionUri = latestSaved?.resumableSessionUri ?: record.resumableSessionUri
            )
            return false
        }
    }

    private suspend fun finalizeAndVerifyUpload(
        accessToken: String,
        record: BackupFileEntity,
        driveFileId: String,
        driveFolderId: String
    ): Boolean {
        val verification = driveApiClient.verifyRemoteFile(
            accessToken = accessToken,
            driveFileId = driveFileId,
            expectedSize = record.fileSize,
            expectedMd5 = record.md5Hash,
            expectedSha256 = record.sha256Hash
        )

        if (verification == VerificationStatus.VERIFICATION_FAILED) {
            backupDao.markUploadFailed(
                id = record.id,
                status = UploadStatus.FAILED,
                reason = FailureReason.VERIFICATION_MISMATCH,
                detail = "Remote Drive file size or checksum did not match local source file",
                sessionUri = null
            )
            return false
        }

        backupDao.markUploadVerified(
            id = record.id,
            driveFileId = driveFileId,
            driveFolderId = driveFolderId,
            fileSize = record.fileSize,
            verificationStatus = verification,
            completedAtMs = System.currentTimeMillis()
        )
        _lastNetworkError.value = null
        return true
    }

    /**
     * Preserves the phone's folder hierarchy inside "Android_Permanent_Backup"
     * (e.g. "DCIM/Camera/IMG_001.jpg" -> creates/reuses "Android_Permanent_Backup/DCIM/Camera").
     */
    private suspend fun resolveDriveSubFolderHierarchy(
        accessToken: String,
        rootFolderId: String,
        relativeFilePath: String
    ): String {
        val normalized = relativeFilePath.replace('\\', '/').trim('/')
        val parentPath = normalized.substringBeforeLast('/', "")
        if (parentPath.isBlank()) return rootFolderId

        backupDao.getCachedDriveFolder(parentPath)?.let { return it.driveFolderId }

        val segments = parentPath.split('/').filter { it.isNotBlank() }
        var currentParentId = rootFolderId
        val pathBuilder = StringBuilder()

        for (segment in segments) {
            if (pathBuilder.isNotEmpty()) pathBuilder.append('/')
            pathBuilder.append(segment)
            val partialPath = pathBuilder.toString()

            val cached = backupDao.getCachedDriveFolder(partialPath)
            if (cached != null) {
                currentParentId = cached.driveFolderId
            } else {
                val folderId = driveApiClient.ensureFolderExists(
                    accessToken = accessToken,
                    folderName = segment,
                    parentFolderId = currentParentId
                )
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

        return currentParentId
    }
}
