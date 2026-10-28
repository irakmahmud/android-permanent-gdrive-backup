package com.permanentbackup.gdrive.data.local

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Update
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus
import kotlinx.coroutines.flow.Flow

/**
 * Append-Only DAO for Permanent Backup Metadata.
 * Notice: No @Delete or "DELETE FROM backup_file_records" exists by architectural design.
 */
@Dao
interface BackupDao {

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
        WHERE uploadStatus IN ('PENDING', 'PENDING_RETRY', 'IN_PROGRESS') 
        ORDER BY createdAtMs ASC 
        LIMIT :limit OFFSET :offset
        """
    )
    suspend fun getPendingRecordsPaged(limit: Int, offset: Int): List<BackupFileEntity>

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
}
