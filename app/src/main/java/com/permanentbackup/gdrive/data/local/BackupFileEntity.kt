package com.permanentbackup.gdrive.data.local

import androidx.room.Entity
import androidx.room.ForeignKey
import androidx.room.Index
import androidx.room.PrimaryKey
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.SyncJobState
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus

/**
 * Append-Only Room Entity representing a specific version of a backed-up file (v1.0–v2.0 compatible).
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

// ============================================================================
// SECTION 8: 12 MANDATORY PRODUCTION ROOM ENTITIES
// ============================================================================

/**
 * 1. GoogleAccountEntity — Persistent Multi-Account Metadata & Authorization State.
 * Raw OAuth refresh tokens are NEVER stored in this SQLite table.
 */
@Entity(
    tableName = "google_accounts",
    indices = [
        Index(value = ["email"], unique = true),
        Index(value = ["isActive"])
    ]
)
data class GoogleAccountEntity(
    @PrimaryKey
    val accountId: String,
    val email: String,
    val displayName: String,
    val selectedAccountRef: String,
    val isActive: Boolean = false,
    val authorizationState: String = "AUTHORIZED",
    val grantedScopes: String = "https://www.googleapis.com/auth/drive.file",
    val lastSuccessfulConnectionMs: Long = 0L,
    val lastTokenRefreshStatus: String = "OK",
    val lastTokenRefreshMs: Long = 0L,
    val connectionError: String? = null,
    val backupRootFolderId: String? = null,
    val backupRootFolderName: String = "Android_Permanent_Backup",
    val createdAtMs: Long = System.currentTimeMillis()
)

/**
 * 2. BackupFolderEntity — Local & Cloud Folder Mapping, Scan State & Per-Folder Backup Control.
 */
@Entity(
    tableName = "backup_folders",
    indices = [
        Index(value = ["accountId", "relativePath"], unique = true),
        Index(value = ["backupEnabled"])
    ]
)
data class BackupFolderEntity(
    @PrimaryKey
    val folderKey: String,
    val accountId: String,
    val folderName: String,
    val relativePath: String,
    val driveFolderId: String? = null,
    val parentDriveFolderId: String? = null,
    val backupEnabled: Boolean = true,
    val isProtectedAndroidFolder: Boolean = false,
    val accessStatus: String = "ACCESSIBLE",
    val discoveredFileCount: Int = 0,
    val backedUpFileCount: Int = 0,
    val lastScanTimeMs: Long = System.currentTimeMillis()
)

/**
 * 3. LocalFileEntity — Indexed Local File Inventory for Fast Incremental Change Detection.
 */
@Entity(
    tableName = "local_files",
    indices = [
        Index(value = ["relativePath"]),
        Index(value = ["folderCategory"]),
        Index(value = ["latestSha256"]),
        Index(value = ["localExists"])
    ]
)
data class LocalFileEntity(
    @PrimaryKey
    val sourceFileIdentity: String,
    val sourceUri: String,
    val relativePath: String,
    val displayName: String,
    val folderCategory: String,
    val fileSize: Long,
    val lastModifiedMs: Long,
    val mimeType: String,
    val latestSha256: String? = null,
    val latestMd5: String? = null,
    val latestVersion: Int = 1,
    val localExists: Boolean = true,
    val lastSeenMs: Long = System.currentTimeMillis()
)

/**
 * 4. CloudFileEntity — Google Drive Cloud File Index, Metadata & Offline Pin State.
 */
@Entity(
    tableName = "cloud_files",
    indices = [
        Index(value = ["accountId", "parentFolderId"]),
        Index(value = ["accountId", "name"]),
        Index(value = ["sha256Checksum"]),
        Index(value = ["isPinnedOffline"]),
        Index(value = ["modifiedTimeMs"])
    ]
)
data class CloudFileEntity(
    @PrimaryKey
    val driveFileId: String,
    val accountId: String,
    val name: String,
    val mimeType: String,
    val sizeBytes: Long,
    val md5Checksum: String? = null,
    val sha256Checksum: String? = null,
    val parentFolderId: String? = null,
    val relativePath: String? = null,
    val isFolder: Boolean = false,
    val isProtectedBackupFile: Boolean = false,
    val starred: Boolean = false,
    val shared: Boolean = false,
    val trashed: Boolean = false,
    val webViewLink: String? = null,
    val createdTimeMs: Long = 0L,
    val modifiedTimeMs: Long = 0L,
    val isPinnedOffline: Boolean = false,
    val offlineLocalPath: String? = null,
    val offlineCachedBytes: Long = 0L,
    val lastSyncedMs: Long = System.currentTimeMillis()
)

/**
 * 5. BackupVersionEntity — Immutable Version History Record per Account & File Identity.
 * Foreign key to LocalFileEntity uses NO_ACTION so local file changes NEVER cascade-delete versions.
 */
@Entity(
    tableName = "backup_versions",
    foreignKeys = [
        ForeignKey(
            entity = LocalFileEntity::class,
            parentColumns = ["sourceFileIdentity"],
            childColumns = ["sourceFileIdentity"],
            onDelete = ForeignKey.NO_ACTION,
            onUpdate = ForeignKey.NO_ACTION
        )
    ],
    indices = [
        Index(value = ["accountId", "sourceFileIdentity", "versionNumber"], unique = true),
        Index(value = ["sourceFileIdentity"]),
        Index(value = ["driveFileId"]),
        Index(value = ["sha256Hash"])
    ]
)
data class BackupVersionEntity(
    @PrimaryKey(autoGenerate = true)
    val versionId: Long = 0L,
    val accountId: String,
    val sourceFileIdentity: String,
    val versionNumber: Int,
    val driveFileId: String,
    val driveFolderId: String? = null,
    val driveStoredName: String,
    val relativePath: String,
    val fileSize: Long,
    val sha256Hash: String? = null,
    val md5Hash: String? = null,
    val verificationStatus: VerificationStatus = VerificationStatus.CHECKSUM_AND_SIZE_VERIFIED,
    val preservedAfterLocalDelete: Boolean = false,
    val createdAtMs: Long = System.currentTimeMillis()
)

/**
 * 6. UploadSessionEntity — Persistent Resumable Upload Session State for Byte-Exact Crash Recovery.
 */
@Entity(
    tableName = "upload_sessions",
    indices = [
        Index(value = ["accountId", "sourceFileIdentity", "backupVersion"], unique = true),
        Index(value = ["recordId"]),
        Index(value = ["isActive"])
    ]
)
data class UploadSessionEntity(
    @PrimaryKey
    val sessionId: String,
    val accountId: String,
    val recordId: Long,
    val sourceFileIdentity: String,
    val backupVersion: Int,
    val resumableSessionUri: String,
    val uploadedBytes: Long,
    val totalBytes: Long,
    val chunkSizeBytes: Int,
    val isActive: Boolean = true,
    val expiresAtMs: Long = System.currentTimeMillis() + 7L * 24L * 3600L * 1000L,
    val updatedAtMs: Long = System.currentTimeMillis()
)

/**
 * 7. SyncJobEntity — Transactional Synchronization Engine State Machine Queue.
 */
@Entity(
    tableName = "sync_jobs",
    indices = [
        Index(value = ["accountId", "sourceFileIdentity", "backupVersion"], unique = true),
        Index(value = ["syncState"]),
        Index(value = ["recordId"]),
        Index(value = ["updatedAtMs"])
    ]
)
data class SyncJobEntity(
    @PrimaryKey(autoGenerate = true)
    val jobId: Long = 0L,
    val accountId: String,
    val recordId: Long,
    val sourceFileIdentity: String,
    val fileName: String,
    val relativePath: String,
    val backupVersion: Int,
    val syncState: SyncJobState = SyncJobState.DISCOVERED,
    val uploadedBytes: Long = 0L,
    val totalBytes: Long = 0L,
    val transferSpeedBps: Long = 0L,
    val isPaused: Boolean = false,
    val retryCount: Int = 0,
    val nextRetryAtMs: Long? = null,
    val errorMessage: String? = null,
    val updatedAtMs: Long = System.currentTimeMillis(),
    val createdAtMs: Long = System.currentTimeMillis()
)

/**
 * 8. SyncEventEntity — Append-Only Audit Log of State Transitions & Sync Operations.
 */
@Entity(
    tableName = "sync_events",
    indices = [
        Index(value = ["accountId", "timestampMs"]),
        Index(value = ["sourceFileIdentity"])
    ]
)
data class SyncEventEntity(
    @PrimaryKey(autoGenerate = true)
    val eventId: Long = 0L,
    val accountId: String,
    val sourceFileIdentity: String? = null,
    val eventType: String,
    val fromState: String? = null,
    val toState: String? = null,
    val message: String,
    val timestampMs: Long = System.currentTimeMillis()
)

/**
 * 9. FailedOperationEntity — Detailed Failure Diagnostics & Retry Scheduling Table.
 */
@Entity(
    tableName = "failed_operations",
    indices = [
        Index(value = ["accountId", "resolved"]),
        Index(value = ["sourceFileIdentity"]),
        Index(value = ["recordId"])
    ]
)
data class FailedOperationEntity(
    @PrimaryKey(autoGenerate = true)
    val failureId: Long = 0L,
    val accountId: String,
    val recordId: Long? = null,
    val sourceFileIdentity: String,
    val operationType: String = "UPLOAD",
    val failureReason: FailureReason,
    val httpStatusCode: Int? = null,
    val errorDetail: String,
    val resumableSessionUri: String? = null,
    val uploadedBytes: Long = 0L,
    val retryable: Boolean = true,
    val retryCount: Int = 1,
    val resolved: Boolean = false,
    val occurredAtMs: Long = System.currentTimeMillis()
)

/**
 * 10. AppSettingsEntity — Persistent Application & Cache Budget Settings in SQLite.
 */
@Entity(tableName = "app_settings")
data class AppSettingsEntity(
    @PrimaryKey
    val settingKey: String = "GLOBAL_SETTINGS",
    val userBackupConsentGranted: Boolean = false,
    val wifiOnly: Boolean = true,
    val chargingOnly: Boolean = false,
    val unmeteredNetworkOnly: Boolean = true,
    val uploadConcurrency: Int = 1,
    val periodicIntervalHours: Int = 6,
    val chunkSizeBytes: Int = 256 * 1024,
    val offlineCacheBudgetBytes: Long = 100L * 1024 * 1024,
    val backgroundSyncPaused: Boolean = false,
    val activeAccountId: String? = null,
    val updatedAtMs: Long = System.currentTimeMillis()
)

/**
 * 11. StoragePermissionEntity — Persistent Record of Granted Storage & SAF Tree Permissions.
 */
@Entity(
    tableName = "storage_permissions",
    indices = [Index(value = ["granted"])]
)
data class StoragePermissionEntity(
    @PrimaryKey
    val permissionKey: String,
    val granted: Boolean,
    val scopeType: String,
    val persistedTreeUri: String? = null,
    val androidSdkLevel: Int,
    val lastCheckedMs: Long = System.currentTimeMillis(),
    val statusExplanation: String
)

/**
 * 12. DriveChangeTokenEntity — Google Drive Changes API v3 Incremental Sync Token per Account.
 */
@Entity(tableName = "drive_change_tokens")
data class DriveChangeTokenEntity(
    @PrimaryKey
    val accountId: String,
    val startPageToken: String,
    val lastChangesPollMs: Long = System.currentTimeMillis(),
    val changesDetectedCount: Int = 0
)
