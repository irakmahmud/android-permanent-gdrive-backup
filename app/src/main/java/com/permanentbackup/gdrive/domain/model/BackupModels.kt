package com.permanentbackup.gdrive.domain.model

/**
 * Strictly Upload-Only, Append-Only Domain Models.
 * No remote delete or destructive sync state exists in this model hierarchy.
 */
enum class UploadStatus {
    PENDING,
    IN_PROGRESS,
    VERIFIED,
    FAILED,
    PENDING_RETRY
}

enum class VerificationStatus {
    UNVERIFIED,
    CHECKSUM_AND_SIZE_VERIFIED,
    SIZE_AND_METADATA_VERIFIED_NO_REMOTE_CHECKSUM,
    VERIFICATION_FAILED
}

enum class FailureReason {
    NONE,
    NETWORK_ERROR,
    STORAGE_QUOTA_FULL,
    PERMISSION_REVOKED,
    AUTH_TOKEN_EXPIRED,
    SOURCE_DELETED_BEFORE_UPLOAD,
    VERIFICATION_MISMATCH,
    SESSION_EXPIRED_RESTARTED
}

enum class FolderAccessStatus {
    ACCESSIBLE,
    PERMISSION_REQUIRED,
    RESTRICTED_BY_ANDROID_OS,
    NOT_PRESENT_ON_DEVICE
}

data class DiscoveredSourceFile(
    val sourceUri: String,
    val sourceFileIdentity: String,
    val relativePath: String,
    val displayName: String,
    val folderCategory: String,
    val fileSize: Long,
    val lastModifiedMs: Long,
    val mimeType: String,
    val sha256Hash: String?,
    val md5Hash: String?
)

data class DriveQuotaInfo(
    val limitBytes: Long = 15L * 1024 * 1024 * 1024,
    val usageBytes: Long = 0L,
    val usageInDriveBytes: Long = 0L,
    val usageInTrashBytes: Long = 0L
) {
    val availableBytes: Long
        get() = (limitBytes - usageBytes).coerceAtLeast(0L)

    val isQuotaFull: Boolean
        get() = limitBytes > 0L && availableBytes <= 0L
}

data class GoogleAccountState(
    val isConnected: Boolean = false,
    val email: String = "",
    val displayName: String = "",
    val grantedScope: String = "https://www.googleapis.com/auth/drive.file",
    val rootDriveFolderId: String? = null,
    val rootDriveFolderName: String = "Android_Permanent_Backup",
    val tokenExpiresAtMs: Long = 0L,
    val lastTokenRefreshMs: Long = 0L,
    val connectionError: String? = null
)

data class BackupSettings(
    val userBackupConsentGranted: Boolean = false,
    val wifiOnly: Boolean = true,
    val chargingOnly: Boolean = false,
    val unmeteredNetworkOnly: Boolean = true,
    val uploadConcurrency: Int = 1,
    val periodicIntervalHours: Int = 6,
    val chunkSizeBytes: Int = 256 * 1024 // 256 KiB multiple required by Google Drive Resumable Upload API
)

data class FolderCoverageItem(
    val folderName: String,
    val relativePath: String,
    val accessStatus: FolderAccessStatus,
    val discoveredFileCount: Int,
    val backedUpFileCount: Int,
    val restrictionExplanation: String
)

data class ActiveUploadProgress(
    val recordId: Long,
    val fileName: String,
    val relativePath: String,
    val backupVersion: Int,
    val uploadedBytes: Long,
    val totalBytes: Long,
    val resumableSessionActive: Boolean
) {
    val fraction: Float
        get() = if (totalBytes > 0L) (uploadedBytes.toFloat() / totalBytes.toFloat()).coerceIn(0f, 1f) else 1f
}

data class DashboardSummary(
    val accountState: GoogleAccountState = GoogleAccountState(),
    val totalDiscoveredFiles: Int = 0,
    val successfullyBackedUpFiles: Int = 0,
    val pendingFiles: Int = 0,
    val failedFiles: Int = 0,
    val preservedAfterLocalDeleteCount: Int = 0,
    val totalUploadedBytes: Long = 0L,
    val overallProgressFraction: Float = 0f,
    val lastSuccessfulBackupMs: Long? = null,
    val nextScheduledBackupMs: Long? = null,
    val driveQuota: DriveQuotaInfo = DriveQuotaInfo(),
    val accessibleFolderCount: Int = 0,
    val restrictedFolderCount: Int = 3,
    val currentlyUploading: ActiveUploadProgress? = null,
    val permissionFailureMessage: String? = null,
    val quotaFullAlert: Boolean = false,
    val networkFailureMessage: String? = null,
    val isBackupRunning: Boolean = false
)

class NeverDeletePolicyViolationException(message: String) : SecurityException(message)

class SourceFileModificationForbiddenException(message: String) : SecurityException(message)

class StorageQuotaFullException(message: String) : Exception(message)

class UploadSessionExpiredException(message: String) : Exception(message)

class AuthTokenExpiredException(message: String) : Exception(message)
