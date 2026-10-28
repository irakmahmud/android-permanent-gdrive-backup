package com.permanentbackup.gdrive.domain.model

/**
 * Strictly Upload-Only, Append-Only Domain Models for Permanent Drive —
 * Native Android Google Drive Desktop Alternative.
 * No remote delete or destructive sync state exists in this model hierarchy.
 */
enum class UploadStatus {
    PENDING,
    IN_PROGRESS,
    VERIFIED,
    FAILED,
    PENDING_RETRY
}

/**
 * Desktop-Class Synchronization Engine State Machine (Section 7):
 * DISCOVERED -> QUEUED -> PREPARING -> UPLOADING -> VERIFYING -> COMPLETED
 * Retryable Error: FAILED_RETRYABLE -> RETRY_SCHEDULED
 * Action Required: FAILED_ACTION_REQUIRED
 */
enum class SyncJobState {
    DISCOVERED,
    QUEUED,
    PREPARING,
    UPLOADING,
    VERIFYING,
    COMPLETED,
    FAILED_RETRYABLE,
    RETRY_SCHEDULED,
    FAILED_ACTION_REQUIRED
}

/**
 * Explicit Background Synchronization Status visible to the user (Section 11).
 */
enum class BackgroundSyncStatus(val label: String) {
    BACKUP_RUNNING("Backup Running"),
    BACKUP_PAUSED("Backup Paused"),
    WAITING_FOR_NETWORK("Waiting for Network"),
    WAITING_FOR_CHARGING("Waiting for Charging"),
    WAITING_FOR_ANDROID_SCHEDULER("Waiting for Android Scheduler"),
    AUTHORIZATION_REQUIRED("Authorization Required"),
    STORAGE_FULL("Storage Full"),
    BACKUP_COMPLETED("Backup Completed")
}

enum class AccountAuthState {
    AUTHORIZED,
    SILENT_REFRESH_OK,
    AUTHORIZATION_REQUIRED,
    SCOPE_LIMITED,
    DISCONNECTED
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
    SESSION_EXPIRED_RESTARTED,
    BAD_REQUEST,
    RATE_LIMITED
}

enum class FolderAccessStatus {
    ACCESSIBLE,
    PERMISSION_REQUIRED,
    RESTRICTED_BY_ANDROID_OS,
    NOT_PRESENT_ON_DEVICE
}

enum class DriveViewFilter(val title: String) {
    BACKUP_ROOT("Backup Root"),
    MY_DRIVE("My Drive"),
    RECENT("Recent"),
    STARRED("Starred"),
    SHARED_WITH_ME("Shared"),
    SHARED_DRIVES("Shared Drives")
}

enum class DriveSortOrder(val title: String, val driveOrderByParam: String) {
    MODIFIED_DESC("Modified Date", "modifiedTime desc"),
    UPLOAD_DATE_DESC("Upload Date", "createdTime desc"),
    NAME_ASC("Name (A-Z)", "name"),
    SIZE_DESC("Size (Largest)", "quotaBytesUsed desc")
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
    val accountId: String = "",
    val isConnected: Boolean = false,
    val email: String = "",
    val displayName: String = "",
    val selectedAccountRef: String = "",
    val authState: AccountAuthState = AccountAuthState.DISCONNECTED,
    val grantedScope: String = "https://www.googleapis.com/auth/drive.file",
    val hasFullDriveReadScope: Boolean = false,
    val rootDriveFolderId: String? = null,
    val rootDriveFolderName: String = "Android_Permanent_Backup",
    val tokenExpiresAtMs: Long = 0L,
    val lastTokenRefreshMs: Long = 0L,
    val lastTokenRefreshStatus: String = "NONE",
    val lastSuccessfulConnectionMs: Long = 0L,
    val connectionError: String? = null
)

data class BackupSettings(
    val userBackupConsentGranted: Boolean = false,
    val wifiOnly: Boolean = true,
    val chargingOnly: Boolean = false,
    val unmeteredNetworkOnly: Boolean = true,
    val uploadConcurrency: Int = 1,
    val periodicIntervalHours: Int = 6,
    val chunkSizeBytes: Int = 256 * 1024, // 256 KiB multiple required by Google Drive Resumable Upload API
    val offlineCacheBudgetBytes: Long = 100L * 1024 * 1024, // Default low-storage 100 MB offline budget
    val backgroundSyncPaused: Boolean = false
)

data class FolderCoverageItem(
    val folderName: String,
    val relativePath: String,
    val accessStatus: FolderAccessStatus,
    val discoveredFileCount: Int,
    val backedUpFileCount: Int,
    val restrictionExplanation: String,
    val backupEnabled: Boolean = true,
    val lastScanTimeMs: Long? = null
) {
    val completenessFraction: Float
        get() = if (discoveredFileCount > 0) {
            (backedUpFileCount.toFloat() / discoveredFileCount.toFloat()).coerceIn(0f, 1f)
        } else if (accessStatus == FolderAccessStatus.ACCESSIBLE) {
            1f
        } else {
            0f
        }
}

data class ActiveUploadProgress(
    val recordId: Long,
    val fileName: String,
    val relativePath: String,
    val backupVersion: Int,
    val uploadedBytes: Long,
    val totalBytes: Long,
    val resumableSessionActive: Boolean,
    val transferSpeedBps: Long = 0L,
    val estimatedRemainingSeconds: Long? = null,
    val syncJobState: SyncJobState = SyncJobState.UPLOADING,
    val isPaused: Boolean = false
) {
    val fraction: Float
        get() = if (totalBytes > 0L) (uploadedBytes.toFloat() / totalBytes.toFloat()).coerceIn(0f, 1f) else 1f
}

data class CloudBrowseItem(
    val driveFileId: String,
    val accountId: String,
    val name: String,
    val mimeType: String,
    val sizeBytes: Long,
    val md5Checksum: String? = null,
    val sha256Checksum: String? = null,
    val parentFolderId: String? = null,
    val isFolder: Boolean = mimeType == "application/vnd.google-apps.folder",
    val isProtectedBackupFile: Boolean = false,
    val starred: Boolean = false,
    val shared: Boolean = false,
    val webViewLink: String? = null,
    val createdTimeMs: Long = 0L,
    val modifiedTimeMs: Long = 0L,
    val isPinnedOffline: Boolean = false,
    val offlineLocalPath: String? = null
)

data class LocalBrowseItem(
    val absolutePath: String,
    val relativePath: String,
    val name: String,
    val isDirectory: Boolean,
    val sizeBytes: Long,
    val lastModifiedMs: Long,
    val mimeType: String,
    val folderCategory: String,
    val isRestrictedByOs: Boolean = false,
    val backupEnabled: Boolean = true,
    val childCount: Int = 0
)

data class OAuthDiagnosticReport(
    val packageName: String,
    val signingCertSha1: String,
    val signingCertSha256: String,
    val activeAccountEmail: String,
    val authorizationState: AccountAuthState,
    val grantedScopes: List<String>,
    val fullMyDriveReadEnabled: Boolean,
    val legacyCompromisedTokensPurged: Boolean,
    val embeddedSecretsInApk: Boolean = false
)

data class RestoreOutcome(
    val success: Boolean,
    val restoredLocalPath: String?,
    val restoredBytes: Long,
    val message: String,
    val avoidedOverwrite: Boolean = false
)

data class DashboardSummary(
    val accountState: GoogleAccountState = GoogleAccountState(),
    val connectedAccountsCount: Int = 0,
    val totalDiscoveredFiles: Int = 0,
    val successfullyBackedUpFiles: Int = 0,
    val pendingFiles: Int = 0,
    val failedFiles: Int = 0,
    val preservedAfterLocalDeleteCount: Int = 0,
    val totalUploadedBytes: Long = 0L,
    val currentUploadSpeedBps: Long = 0L,
    val overallProgressFraction: Float = 0f,
    val lastSuccessfulBackupMs: Long? = null,
    val nextScheduledBackupMs: Long? = null,
    val driveQuota: DriveQuotaInfo = DriveQuotaInfo(),
    val accessibleFolderCount: Int = 0,
    val restrictedFolderCount: Int = 3,
    val currentlyUploading: ActiveUploadProgress? = null,
    val backgroundSyncStatus: BackgroundSyncStatus = BackgroundSyncStatus.WAITING_FOR_ANDROID_SCHEDULER,
    val offlinePinnedCount: Int = 0,
    val offlineCacheUsedBytes: Long = 0L,
    val offlineCacheBudgetBytes: Long = 100L * 1024 * 1024,
    val permissionFailureMessage: String? = null,
    val quotaFullAlert: Boolean = false,
    val networkFailureMessage: String? = null,
    val isBackupRunning: Boolean = false,
    val dbIntegrityOk: Boolean = true
)

class NeverDeletePolicyViolationException(message: String) : SecurityException(message)

class SourceFileModificationForbiddenException(message: String) : SecurityException(message)

class StorageQuotaFullException(message: String) : Exception(message)

class UploadSessionExpiredException(message: String) : Exception(message)

class AuthTokenExpiredException(message: String) : Exception(message)

class DrivePermissionException(message: String) : Exception(message)

class DriveBadRequestException(
    val requestMethod: String,
    val sanitizedUrl: String,
    val queryParamNames: List<String>,
    val sanitizedResponse: String,
    message: String = "Google Drive HTTP 400 Bad Request ($requestMethod $sanitizedUrl, params=$queryParamNames): $sanitizedResponse"
) : Exception(message)

class DriveRateLimitException(message: String) : Exception(message)
