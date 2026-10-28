package com.permanentbackup.gdrive.data.local

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus

/**
 * Append-Only Room Entity representing a specific version of a backed-up file.
 * Existing verified rows are never deleted or overwritten when a local file changes or is deleted.
 */
@Entity(
    tableName = "backup_file_records",
    indices = [
        Index(value = ["sourceFileIdentity", "backupVersion"], unique = true),
        Index(value = ["sourceUri"]),
        Index(value = ["uploadStatus"]),
        Index(value = ["sha256Hash"]),
        Index(value = ["driveFileId"])
    ]
)
data class BackupFileEntity(
    @PrimaryKey(autoGenerate = true)
    val id: Long = 0L,
    val sourceUri: String,
    val sourceFileIdentity: String,
    val relativePath: String,
    val displayName: String,
    val folderCategory: String,
    val fileSize: Long,
    val lastModifiedMs: Long,
    val mimeType: String,
    val sha256Hash: String?,
    val md5Hash: String?,
    val driveFileId: String? = null,
    val driveFolderId: String? = null,
    val driveStoredName: String,
    val backupVersion: Int = 1,
    val uploadedBytes: Long = 0L,
    val resumableSessionUri: String? = null,
    val uploadStatus: UploadStatus = UploadStatus.PENDING,
    val lastSuccessfulBackupMs: Long? = null,
    val failureReason: FailureReason = FailureReason.NONE,
    val failureDetail: String? = null,
    val verificationStatus: VerificationStatus = VerificationStatus.UNVERIFIED,
    val localExists: Boolean = true,
    val retryCount: Int = 0,
    val createdAtMs: Long = System.currentTimeMillis()
)

@Entity(
    tableName = "drive_folder_cache",
    indices = [Index(value = ["relativePath"], unique = true)]
)
data class DriveFolderCacheEntity(
    @PrimaryKey
    val relativePath: String,
    val driveFolderId: String,
    val parentDriveFolderId: String,
    val createdAtMs: Long = System.currentTimeMillis()
)
