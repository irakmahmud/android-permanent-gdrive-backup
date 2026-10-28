package com.permanentbackup.gdrive.data.local

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Transaction
import androidx.room.Update
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.SyncJobState
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus
import kotlinx.coroutines.flow.Flow

/**
 * Append-Only & Transactionally Safe DAO for Permanent Backup Metadata & All 12 Room Entities.
 * Notice: No @Delete or destructive "DELETE FROM backup_file_records / backup_versions" exists
 * by architectural design (Section 6 & Section 8).
 */
@Dao
interface BackupDao {

    // ========================================================================
    // Core Backup File Records (v1.0 - v2.0 Append-Only History)
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertRecord(record: BackupFileEntity): Long

    @Update
    suspend fun updateRecord(record: BackupFileEntity)

    @Query("SELECT * FROM backup_file_records WHERE id = :id LIMIT 1")
    suspend fun getRecordById(id: Long): BackupFileEntity?

    @Query(
        """
        SELECT * FROM backup_file_records 
        WHERE sourceFileIdentity = :identity 
        ORDER BY backupVersion DESC 
        LIMIT 1
        """
    )
    suspend fun getLatestVersionForIdentity(identity: String): BackupFileEntity?

    @Query(
        """
        SELECT * FROM backup_file_records 
        WHERE sourceFileIdentity = :identity 
        ORDER BY backupVersion DESC
        """
    )
    suspend fun getAllVersionsForIdentity(identity: String): List<BackupFileEntity>

    @Query(
        """
        SELECT * FROM backup_file_records 
        WHERE uploadStatus IN ('PENDING', 'PENDING_RETRY', 'IN_PROGRESS') 
        ORDER BY createdAtMs ASC 
        LIMIT :limit OFFSET :offset
        """
    )
    suspend fun getPendingRecordsPaged(limit: Int, offset: Int): List<BackupFileEntity>

    @Query(
        """
        SELECT * FROM backup_file_records 
        WHERE uploadStatus = 'FAILED'
        ORDER BY createdAtMs ASC
        LIMIT :limit OFFSET :offset
        """
    )
    suspend fun getFailedRecordsPaged(limit: Int, offset: Int): List<BackupFileEntity>

    @Query(
        """
        SELECT * FROM backup_file_records 
        WHERE uploadStatus = 'IN_PROGRESS'
        """
    )
    suspend fun getInProgressRecords(): List<BackupFileEntity>

    @Query(
        """
        UPDATE backup_file_records 
        SET uploadedBytes = :uploadedBytes, 
            resumableSessionUri = :sessionUri, 
            uploadStatus = :status 
        WHERE id = :id
        """
    )
    suspend fun updateUploadProgress(
        id: Long,
        uploadedBytes: Long,
        sessionUri: String?,
        status: UploadStatus
    )

    @Query(
        """
        UPDATE backup_file_records 
        SET driveFileId = :driveFileId,
            driveFolderId = :driveFolderId,
            uploadedBytes = :fileSize,
            resumableSessionUri = NULL,
            uploadStatus = 'VERIFIED',
            verificationStatus = :verificationStatus,
            lastSuccessfulBackupMs = :completedAtMs,
            failureReason = 'NONE',
            failureDetail = NULL
        WHERE id = :id
        """
    )
    suspend fun markUploadVerified(
        id: Long,
        driveFileId: String,
        driveFolderId: String?,
        fileSize: Long,
        verificationStatus: VerificationStatus,
        completedAtMs: Long
    )

    @Query(
        """
        UPDATE backup_file_records 
        SET uploadStatus = :status,
            failureReason = :reason,
            failureDetail = :detail,
            resumableSessionUri = :sessionUri,
            retryCount = retryCount + 1
        WHERE id = :id
        """
    )
    suspend fun markUploadFailed(
        id: Long,
        status: UploadStatus,
        reason: FailureReason,
        detail: String?,
        sessionUri: String?
    )

    @Query(
        """
        UPDATE backup_file_records 
        SET localExists = 0 
        WHERE sourceFileIdentity = :identity
        """
    )
    suspend fun markLocalFileDeletedPreservingCloudBackup(identity: String)

    @Query("SELECT * FROM backup_file_records ORDER BY createdAtMs DESC LIMIT 500")
    fun observeRecentRecords(): Flow<List<BackupFileEntity>>

    @Query("SELECT * FROM backup_file_records WHERE uploadStatus = 'VERIFIED' ORDER BY lastSuccessfulBackupMs DESC LIMIT 500")
    fun observeVerifiedHistory(): Flow<List<BackupFileEntity>>

    @Query("SELECT * FROM backup_file_records WHERE uploadStatus IN ('FAILED', 'PENDING_RETRY') ORDER BY createdAtMs DESC")
    fun observeFailedRecords(): Flow<List<BackupFileEntity>>

    @Query("SELECT * FROM backup_file_records WHERE uploadStatus IN ('PENDING', 'IN_PROGRESS', 'PENDING_RETRY') ORDER BY createdAtMs ASC")
    fun observeActiveQueue(): Flow<List<BackupFileEntity>>

    @Query("SELECT COUNT(*) FROM backup_file_records")
    suspend fun countAllRecords(): Int

    @Query("SELECT COUNT(*) FROM backup_file_records WHERE uploadStatus = 'VERIFIED'")
    suspend fun countVerifiedRecords(): Int

    @Query("SELECT COUNT(DISTINCT sourceFileIdentity) FROM backup_file_records WHERE uploadStatus = 'VERIFIED' AND folderCategory = :category")
    suspend fun countVerifiedFilesInCategory(category: String): Int

    @Query("SELECT COUNT(*) FROM backup_file_records WHERE uploadStatus IN ('PENDING', 'PENDING_RETRY', 'IN_PROGRESS')")
    suspend fun countPendingRecords(): Int

    @Query("SELECT COUNT(*) FROM backup_file_records WHERE uploadStatus = 'FAILED'")
    suspend fun countFailedRecords(): Int

    @Query("SELECT COUNT(*) FROM backup_file_records WHERE uploadStatus = 'VERIFIED' AND localExists = 0")
    suspend fun countPreservedAfterLocalDelete(): Int

    @Query("SELECT COALESCE(SUM(fileSize), 0) FROM backup_file_records WHERE uploadStatus = 'VERIFIED'")
    suspend fun sumTotalVerifiedUploadedBytes(): Long

    @Query("SELECT MAX(lastSuccessfulBackupMs) FROM backup_file_records WHERE uploadStatus = 'VERIFIED'")
    suspend fun getLastSuccessfulBackupTimestamp(): Long?

    @Query("SELECT DISTINCT sourceFileIdentity FROM backup_file_records WHERE localExists = 1 LIMIT :limit OFFSET :offset")
    suspend fun getActiveIdentitiesPaged(limit: Int, offset: Int): List<String>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun cacheDriveFolder(folder: DriveFolderCacheEntity)

    @Query("SELECT * FROM drive_folder_cache WHERE relativePath = :relativePath LIMIT 1")
    suspend fun getCachedDriveFolder(relativePath: String): DriveFolderCacheEntity?

    // ========================================================================
    // 1. GoogleAccountEntity — Multi-Account & Persistent Login State
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertGoogleAccount(account: GoogleAccountEntity)

    @Query("SELECT * FROM google_accounts WHERE isActive = 1 LIMIT 1")
    suspend fun getActiveGoogleAccount(): GoogleAccountEntity?

    @Query("SELECT * FROM google_accounts WHERE accountId = :accountId LIMIT 1")
    suspend fun getGoogleAccountById(accountId: String): GoogleAccountEntity?

    @Query("SELECT * FROM google_accounts ORDER BY lastSuccessfulConnectionMs DESC")
    suspend fun getAllGoogleAccounts(): List<GoogleAccountEntity>

    @Query("SELECT * FROM google_accounts ORDER BY lastSuccessfulConnectionMs DESC")
    fun observeAllGoogleAccounts(): Flow<List<GoogleAccountEntity>>

    @Query("UPDATE google_accounts SET isActive = CASE WHEN accountId = :activeAccountId THEN 1 ELSE 0 END")
    suspend fun setActiveGoogleAccount(activeAccountId: String)

    // ========================================================================
    // 2. BackupFolderEntity — Folder Management & Per-Folder Backup Toggles
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertBackupFolder(folder: BackupFolderEntity)

    @Query("SELECT * FROM backup_folders WHERE accountId = :accountId ORDER BY folderName ASC")
    suspend fun getBackupFoldersForAccount(accountId: String): List<BackupFolderEntity>

    @Query("SELECT * FROM backup_folders WHERE folderKey = :folderKey LIMIT 1")
    suspend fun getBackupFolderByKey(folderKey: String): BackupFolderEntity?

    // ========================================================================
    // 3. LocalFileEntity — Incremental Scan Index
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertLocalFile(localFile: LocalFileEntity)

    @Query("SELECT * FROM local_files WHERE sourceFileIdentity = :identity LIMIT 1")
    suspend fun getLocalFileByIdentity(identity: String): LocalFileEntity?

    @Query("UPDATE local_files SET localExists = 0 WHERE sourceFileIdentity = :identity")
    suspend fun markLocalFileIndexDeleted(identity: String)

    // ========================================================================
    // 4. CloudFileEntity — My Drive Explorer & Offline Access Index
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertCloudFile(cloudFile: CloudFileEntity)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertCloudFiles(cloudFiles: List<CloudFileEntity>)

    @Query("SELECT * FROM cloud_files WHERE driveFileId = :driveFileId LIMIT 1")
    suspend fun getCloudFileById(driveFileId: String): CloudFileEntity?

    @Query(
        """
        SELECT * FROM cloud_files 
        WHERE accountId = :accountId AND trashed = 0
        ORDER BY isFolder DESC, modifiedTimeMs DESC 
        LIMIT :limit OFFSET :offset
        """
    )
    suspend fun getCloudFilesForAccountPaged(accountId: String, limit: Int, offset: Int): List<CloudFileEntity>

    @Query(
        """
        SELECT * FROM cloud_files 
        WHERE accountId = :accountId AND isPinnedOffline = 1 AND trashed = 0
        ORDER BY modifiedTimeMs DESC
        """
    )
    fun observePinnedOfflineFiles(accountId: String): Flow<List<CloudFileEntity>>

    @Query("SELECT COUNT(*) FROM cloud_files WHERE accountId = :accountId AND isPinnedOffline = 1")
    suspend fun countPinnedOfflineFiles(accountId: String): Int

    @Query("SELECT COALESCE(SUM(offlineCachedBytes), 0) FROM cloud_files WHERE isPinnedOffline = 1")
    suspend fun sumOfflineCachedBytes(): Long

    @Query(
        """
        UPDATE cloud_files 
        SET isPinnedOffline = :pinned, 
            offlineLocalPath = :localPath, 
            offlineCachedBytes = :cachedBytes 
        WHERE driveFileId = :driveFileId
        """
    )
    suspend fun updateCloudFileOfflinePin(
        driveFileId: String,
        pinned: Boolean,
        localPath: String?,
        cachedBytes: Long
    )

    // ========================================================================
    // 5. BackupVersionEntity — Immutable Version History
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertBackupVersion(version: BackupVersionEntity): Long

    @Query(
        """
        SELECT * FROM backup_versions 
        WHERE sourceFileIdentity = :identity 
        ORDER BY versionNumber DESC
        """
    )
    suspend fun getBackupVersionsForIdentity(identity: String): List<BackupVersionEntity>

    @Query(
        """
        SELECT * FROM backup_versions 
        WHERE accountId = :accountId 
        ORDER BY createdAtMs DESC 
        LIMIT :limit OFFSET :offset
        """
    )
    suspend fun getBackupVersionsForAccountPaged(accountId: String, limit: Int, offset: Int): List<BackupVersionEntity>

    @Query(
        """
        UPDATE backup_versions 
        SET preservedAfterLocalDelete = 1 
        WHERE sourceFileIdentity = :identity
        """
    )
    suspend fun markVersionsPreservedAfterLocalDelete(identity: String)

    // ========================================================================
    // 6. UploadSessionEntity — Resumable Session Tracking
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertUploadSession(session: UploadSessionEntity)

    @Query(
        """
        SELECT * FROM upload_sessions 
        WHERE accountId = :accountId AND sourceFileIdentity = :identity AND backupVersion = :version AND isActive = 1 
        LIMIT 1
        """
    )
    suspend fun getActiveUploadSession(accountId: String, identity: String, version: Int): UploadSessionEntity?

    @Query("UPDATE upload_sessions SET isActive = 0, updatedAtMs = :nowMs WHERE sessionId = :sessionId")
    suspend fun deactivateUploadSession(sessionId: String, nowMs: Long = System.currentTimeMillis())

    // ========================================================================
    // 7 & 8. SyncJobEntity & SyncEventEntity — Transactional State Machine
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertSyncJob(job: SyncJobEntity): Long

    @Query("SELECT * FROM sync_jobs WHERE recordId = :recordId LIMIT 1")
    suspend fun getSyncJobByRecordId(recordId: Long): SyncJobEntity?

    @Query("SELECT * FROM sync_jobs ORDER BY updatedAtMs DESC LIMIT 500")
    fun observeSyncJobs(): Flow<List<SyncJobEntity>>

    @Query("UPDATE sync_jobs SET isPaused = :paused, updatedAtMs = :nowMs WHERE recordId = :recordId")
    suspend fun setSyncJobPaused(recordId: Long, paused: Boolean, nowMs: Long = System.currentTimeMillis())

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertSyncEvent(event: SyncEventEntity): Long

    @Query("SELECT * FROM sync_events ORDER BY timestampMs DESC LIMIT 200")
    fun observeRecentSyncEvents(): Flow<List<SyncEventEntity>>

    @Transaction
    suspend fun transitionSyncJobState(
        accountId: String,
        recordId: Long,
        sourceFileIdentity: String,
        fileName: String,
        relativePath: String,
        backupVersion: Int,
        newState: SyncJobState,
        uploadedBytes: Long,
        totalBytes: Long,
        transferSpeedBps: Long = 0L,
        errorMessage: String? = null
    ) {
        val now = System.currentTimeMillis()
        val existing = getSyncJobByRecordId(recordId)
        val fromState = existing?.syncState?.name
        val retryCount = if (newState == SyncJobState.FAILED_RETRYABLE || newState == SyncJobState.RETRY_SCHEDULED) {
            (existing?.retryCount ?: 0) + 1
        } else {
            existing?.retryCount ?: 0
        }
        val nextRetryAt = if (newState == SyncJobState.RETRY_SCHEDULED) {
            val expShift = (retryCount - 1).coerceIn(0, 6)
            now + ((1L shl expShift) * 30_000L)
        } else {
            null
        }
        upsertSyncJob(
            SyncJobEntity(
                jobId = existing?.jobId ?: 0L,
                accountId = accountId,
                recordId = recordId,
                sourceFileIdentity = sourceFileIdentity,
                fileName = fileName,
                relativePath = relativePath,
                backupVersion = backupVersion,
                syncState = newState,
                uploadedBytes = uploadedBytes,
                totalBytes = totalBytes,
                transferSpeedBps = transferSpeedBps,
                isPaused = existing?.isPaused ?: false,
                retryCount = retryCount,
                nextRetryAtMs = nextRetryAt,
                errorMessage = errorMessage,
                updatedAtMs = now,
                createdAtMs = existing?.createdAtMs ?: now
            )
        )
        insertSyncEvent(
            SyncEventEntity(
                accountId = accountId,
                sourceFileIdentity = sourceFileIdentity,
                eventType = "STATE_TRANSITION",
                fromState = fromState,
                toState = newState.name,
                message = errorMessage ?: "$fileName (v$backupVersion) -> ${newState.name}",
                timestampMs = now
            )
        )
    }

    // ========================================================================
    // 9. FailedOperationEntity — Failure Diagnostics
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertFailedOperation(failedOp: FailedOperationEntity): Long

    @Query("SELECT * FROM failed_operations WHERE resolved = 0 ORDER BY occurredAtMs DESC LIMIT 200")
    fun observeUnresolvedFailures(): Flow<List<FailedOperationEntity>>

    @Query("UPDATE failed_operations SET resolved = 1 WHERE recordId = :recordId")
    suspend fun markFailuresResolvedForRecord(recordId: Long)

    // ========================================================================
    // 10, 11 & 12. AppSettingsEntity, StoragePermissionEntity, DriveChangeTokenEntity
    // ========================================================================

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAppSettings(settings: AppSettingsEntity)

    @Query("SELECT * FROM app_settings WHERE settingKey = 'GLOBAL_SETTINGS' LIMIT 1")
    suspend fun getAppSettingsEntity(): AppSettingsEntity?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertStoragePermission(permission: StoragePermissionEntity)

    @Query("SELECT * FROM storage_permissions ORDER BY permissionKey ASC")
    suspend fun getAllStoragePermissions(): List<StoragePermissionEntity>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertDriveChangeToken(token: DriveChangeTokenEntity)

    @Query("SELECT * FROM drive_change_tokens WHERE accountId = :accountId LIMIT 1")
    suspend fun getDriveChangeToken(accountId: String): DriveChangeTokenEntity?
}
