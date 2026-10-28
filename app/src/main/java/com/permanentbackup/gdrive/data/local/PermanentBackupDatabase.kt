package com.permanentbackup.gdrive.data.local

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.TypeConverter
import androidx.room.TypeConverters
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.SyncJobState
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus

class BackupTypeConverters {
    @TypeConverter
    fun fromUploadStatus(value: UploadStatus): String = value.name

    @TypeConverter
    fun toUploadStatus(value: String): UploadStatus = UploadStatus.valueOf(value)

    @TypeConverter
    fun fromVerificationStatus(value: VerificationStatus): String = value.name

    @TypeConverter
    fun toVerificationStatus(value: String): VerificationStatus = VerificationStatus.valueOf(value)

    @TypeConverter
    fun fromFailureReason(value: FailureReason): String = value.name

    @TypeConverter
    fun toFailureReason(value: String): FailureReason = FailureReason.valueOf(value)

    @TypeConverter
    fun fromSyncJobState(value: SyncJobState): String = value.name

    @TypeConverter
    fun toSyncJobState(value: String): SyncJobState = SyncJobState.valueOf(value)
}

@Database(
    entities = [
        BackupFileEntity::class,
        DriveFolderCacheEntity::class,
        GoogleAccountEntity::class,
        BackupFolderEntity::class,
        LocalFileEntity::class,
        CloudFileEntity::class,
        BackupVersionEntity::class,
        UploadSessionEntity::class,
        SyncJobEntity::class,
        SyncEventEntity::class,
        FailedOperationEntity::class,
        AppSettingsEntity::class,
        StoragePermissionEntity::class,
        DriveChangeTokenEntity::class
    ],
    version = 2,
    exportSchema = false
)
@TypeConverters(BackupTypeConverters::class)
abstract class PermanentBackupDatabase : RoomDatabase() {
    abstract fun backupDao(): BackupDao

    /**
     * Runs SQLite PRAGMA integrity_check to verify database consistency (Section 8).
     */
    fun checkDatabaseIntegrity(): Boolean {
        return try {
            openHelper.writableDatabase.query("PRAGMA integrity_check").use { cursor ->
                if (cursor.moveToFirst()) {
                    val result = cursor.getString(0) ?: ""
                    result.equals("ok", ignoreCase = true)
                } else {
                    false
                }
            }
        } catch (_: Throwable) {
            false
        }
    }

    companion object {
        const val DB_NAME = "permanent_gdrive_backup.db"

        /**
         * Non-Destructive Schema Migration from v1 (v1.0.0–v1.3.0) to v2 (v2.0.0).
         * Preserves 100% of existing backup_file_records and drive_folder_cache rows,
         * creates the 12 new entities, and backfills local_files and backup_versions
         * from existing verified backup records.
         */
        val MIGRATION_1_2 = object : Migration(1, 2) {
            override fun migrate(db: SupportSQLiteDatabase) {
                // 1. google_accounts
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `google_accounts` (
                        `accountId` TEXT NOT NULL,
                        `email` TEXT NOT NULL,
                        `displayName` TEXT NOT NULL,
                        `selectedAccountRef` TEXT NOT NULL,
                        `isActive` INTEGER NOT NULL,
                        `authorizationState` TEXT NOT NULL,
                        `grantedScopes` TEXT NOT NULL,
                        `lastSuccessfulConnectionMs` INTEGER NOT NULL,
                        `lastTokenRefreshStatus` TEXT NOT NULL,
                        `lastTokenRefreshMs` INTEGER NOT NULL,
                        `connectionError` TEXT,
                        `backupRootFolderId` TEXT,
                        `backupRootFolderName` TEXT NOT NULL,
                        `createdAtMs` INTEGER NOT NULL,
                        PRIMARY KEY(`accountId`)
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_google_accounts_email` ON `google_accounts` (`email`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_google_accounts_isActive` ON `google_accounts` (`isActive`)")

                // 2. backup_folders
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `backup_folders` (
                        `folderKey` TEXT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `folderName` TEXT NOT NULL,
                        `relativePath` TEXT NOT NULL,
                        `driveFolderId` TEXT,
                        `parentDriveFolderId` TEXT,
                        `backupEnabled` INTEGER NOT NULL,
                        `isProtectedAndroidFolder` INTEGER NOT NULL,
                        `accessStatus` TEXT NOT NULL,
                        `discoveredFileCount` INTEGER NOT NULL,
                        `backedUpFileCount` INTEGER NOT NULL,
                        `lastScanTimeMs` INTEGER NOT NULL,
                        PRIMARY KEY(`folderKey`)
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_backup_folders_accountId_relativePath` ON `backup_folders` (`accountId`, `relativePath`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_backup_folders_backupEnabled` ON `backup_folders` (`backupEnabled`)")

                // 3. local_files
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `local_files` (
                        `sourceFileIdentity` TEXT NOT NULL,
                        `sourceUri` TEXT NOT NULL,
                        `relativePath` TEXT NOT NULL,
                        `displayName` TEXT NOT NULL,
                        `folderCategory` TEXT NOT NULL,
                        `fileSize` INTEGER NOT NULL,
                        `lastModifiedMs` INTEGER NOT NULL,
                        `mimeType` TEXT NOT NULL,
                        `latestSha256` TEXT,
                        `latestMd5` TEXT,
                        `latestVersion` INTEGER NOT NULL,
                        `localExists` INTEGER NOT NULL,
                        `lastSeenMs` INTEGER NOT NULL,
                        PRIMARY KEY(`sourceFileIdentity`)
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_local_files_relativePath` ON `local_files` (`relativePath`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_local_files_folderCategory` ON `local_files` (`folderCategory`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_local_files_latestSha256` ON `local_files` (`latestSha256`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_local_files_localExists` ON `local_files` (`localExists`)")

                // 4. cloud_files
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `cloud_files` (
                        `driveFileId` TEXT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `name` TEXT NOT NULL,
                        `mimeType` TEXT NOT NULL,
                        `sizeBytes` INTEGER NOT NULL,
                        `md5Checksum` TEXT,
                        `sha256Checksum` TEXT,
                        `parentFolderId` TEXT,
                        `relativePath` TEXT,
                        `isFolder` INTEGER NOT NULL,
                        `isProtectedBackupFile` INTEGER NOT NULL,
                        `starred` INTEGER NOT NULL,
                        `shared` INTEGER NOT NULL,
                        `trashed` INTEGER NOT NULL,
                        `webViewLink` TEXT,
                        `createdTimeMs` INTEGER NOT NULL,
                        `modifiedTimeMs` INTEGER NOT NULL,
                        `isPinnedOffline` INTEGER NOT NULL,
                        `offlineLocalPath` TEXT,
                        `offlineCachedBytes` INTEGER NOT NULL,
                        `lastSyncedMs` INTEGER NOT NULL,
                        PRIMARY KEY(`driveFileId`)
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_cloud_files_accountId_parentFolderId` ON `cloud_files` (`accountId`, `parentFolderId`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_cloud_files_accountId_name` ON `cloud_files` (`accountId`, `name`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_cloud_files_sha256Checksum` ON `cloud_files` (`sha256Checksum`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_cloud_files_isPinnedOffline` ON `cloud_files` (`isPinnedOffline`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_cloud_files_modifiedTimeMs` ON `cloud_files` (`modifiedTimeMs`)")

                // 5. backup_versions
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `backup_versions` (
                        `versionId` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `sourceFileIdentity` TEXT NOT NULL,
                        `versionNumber` INTEGER NOT NULL,
                        `driveFileId` TEXT NOT NULL,
                        `driveFolderId` TEXT,
                        `driveStoredName` TEXT NOT NULL,
                        `relativePath` TEXT NOT NULL,
                        `fileSize` INTEGER NOT NULL,
                        `sha256Hash` TEXT,
                        `md5Hash` TEXT,
                        `verificationStatus` TEXT NOT NULL,
                        `preservedAfterLocalDelete` INTEGER NOT NULL,
                        `createdAtMs` INTEGER NOT NULL,
                        FOREIGN KEY(`sourceFileIdentity`) REFERENCES `local_files`(`sourceFileIdentity`) ON UPDATE NO ACTION ON DELETE NO ACTION
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_backup_versions_accountId_sourceFileIdentity_versionNumber` ON `backup_versions` (`accountId`, `sourceFileIdentity`, `versionNumber`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_backup_versions_sourceFileIdentity` ON `backup_versions` (`sourceFileIdentity`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_backup_versions_driveFileId` ON `backup_versions` (`driveFileId`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_backup_versions_sha256Hash` ON `backup_versions` (`sha256Hash`)")

                // 6. upload_sessions
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `upload_sessions` (
                        `sessionId` TEXT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `recordId` INTEGER NOT NULL,
                        `sourceFileIdentity` TEXT NOT NULL,
                        `backupVersion` INTEGER NOT NULL,
                        `resumableSessionUri` TEXT NOT NULL,
                        `uploadedBytes` INTEGER NOT NULL,
                        `totalBytes` INTEGER NOT NULL,
                        `chunkSizeBytes` INTEGER NOT NULL,
                        `isActive` INTEGER NOT NULL,
                        `expiresAtMs` INTEGER NOT NULL,
                        `updatedAtMs` INTEGER NOT NULL,
                        PRIMARY KEY(`sessionId`)
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_upload_sessions_accountId_sourceFileIdentity_backupVersion` ON `upload_sessions` (`accountId`, `sourceFileIdentity`, `backupVersion`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_upload_sessions_recordId` ON `upload_sessions` (`recordId`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_upload_sessions_isActive` ON `upload_sessions` (`isActive`)")

                // 7. sync_jobs
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `sync_jobs` (
                        `jobId` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `recordId` INTEGER NOT NULL,
                        `sourceFileIdentity` TEXT NOT NULL,
                        `fileName` TEXT NOT NULL,
                        `relativePath` TEXT NOT NULL,
                        `backupVersion` INTEGER NOT NULL,
                        `syncState` TEXT NOT NULL,
                        `uploadedBytes` INTEGER NOT NULL,
                        `totalBytes` INTEGER NOT NULL,
                        `transferSpeedBps` INTEGER NOT NULL,
                        `isPaused` INTEGER NOT NULL,
                        `retryCount` INTEGER NOT NULL,
                        `nextRetryAtMs` INTEGER,
                        `errorMessage` TEXT,
                        `updatedAtMs` INTEGER NOT NULL,
                        `createdAtMs` INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_sync_jobs_accountId_sourceFileIdentity_backupVersion` ON `sync_jobs` (`accountId`, `sourceFileIdentity`, `backupVersion`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_sync_jobs_syncState` ON `sync_jobs` (`syncState`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_sync_jobs_recordId` ON `sync_jobs` (`recordId`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_sync_jobs_updatedAtMs` ON `sync_jobs` (`updatedAtMs`)")

                // 8. sync_events
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `sync_events` (
                        `eventId` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `sourceFileIdentity` TEXT,
                        `eventType` TEXT NOT NULL,
                        `fromState` TEXT,
                        `toState` TEXT,
                        `message` TEXT NOT NULL,
                        `timestampMs` INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_sync_events_accountId_timestampMs` ON `sync_events` (`accountId`, `timestampMs`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_sync_events_sourceFileIdentity` ON `sync_events` (`sourceFileIdentity`)")

                // 9. failed_operations
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `failed_operations` (
                        `failureId` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        `accountId` TEXT NOT NULL,
                        `recordId` INTEGER,
                        `sourceFileIdentity` TEXT NOT NULL,
                        `operationType` TEXT NOT NULL,
                        `failureReason` TEXT NOT NULL,
                        `httpStatusCode` INTEGER,
                        `errorDetail` TEXT NOT NULL,
                        `resumableSessionUri` TEXT,
                        `uploadedBytes` INTEGER NOT NULL,
                        `retryable` INTEGER NOT NULL,
                        `retryCount` INTEGER NOT NULL,
                        `resolved` INTEGER NOT NULL,
                        `occurredAtMs` INTEGER NOT NULL
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_failed_operations_accountId_resolved` ON `failed_operations` (`accountId`, `resolved`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_failed_operations_sourceFileIdentity` ON `failed_operations` (`sourceFileIdentity`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_failed_operations_recordId` ON `failed_operations` (`recordId`)")

                // 10. app_settings
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `app_settings` (
                        `settingKey` TEXT NOT NULL,
                        `userBackupConsentGranted` INTEGER NOT NULL,
                        `wifiOnly` INTEGER NOT NULL,
                        `chargingOnly` INTEGER NOT NULL,
                        `unmeteredNetworkOnly` INTEGER NOT NULL,
                        `uploadConcurrency` INTEGER NOT NULL,
                        `periodicIntervalHours` INTEGER NOT NULL,
                        `chunkSizeBytes` INTEGER NOT NULL,
                        `offlineCacheBudgetBytes` INTEGER NOT NULL,
                        `backgroundSyncPaused` INTEGER NOT NULL,
                        `activeAccountId` TEXT,
                        `updatedAtMs` INTEGER NOT NULL,
                        PRIMARY KEY(`settingKey`)
                    )
                    """.trimIndent()
                )

                // 11. storage_permissions
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `storage_permissions` (
                        `permissionKey` TEXT NOT NULL,
                        `granted` INTEGER NOT NULL,
                        `scopeType` TEXT NOT NULL,
                        `persistedTreeUri` TEXT,
                        `androidSdkLevel` INTEGER NOT NULL,
                        `lastCheckedMs` INTEGER NOT NULL,
                        `statusExplanation` TEXT NOT NULL,
                        PRIMARY KEY(`permissionKey`)
                    )
                    """.trimIndent()
                )
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_storage_permissions_granted` ON `storage_permissions` (`granted`)")

                // 12. drive_change_tokens
                db.execSQL(
                    """
                    CREATE TABLE IF NOT EXISTS `drive_change_tokens` (
                        `accountId` TEXT NOT NULL,
                        `startPageToken` TEXT NOT NULL,
                        `lastChangesPollMs` INTEGER NOT NULL,
                        `changesDetectedCount` INTEGER NOT NULL,
                        PRIMARY KEY(`accountId`)
                    )
                    """.trimIndent()
                )

                // Backfill local_files from existing v1.3.0 backup_file_records without losing any history
                db.execSQL(
                    """
                    INSERT OR IGNORE INTO `local_files` (
                        `sourceFileIdentity`, `sourceUri`, `relativePath`, `displayName`,
                        `folderCategory`, `fileSize`, `lastModifiedMs`, `mimeType`,
                        `latestSha256`, `latestMd5`, `latestVersion`, `localExists`, `lastSeenMs`
                    )
                    SELECT `sourceFileIdentity`, `sourceUri`, `relativePath`, `displayName`,
                           `folderCategory`, `fileSize`, `lastModifiedMs`, `mimeType`,
                           `sha256Hash`, `md5Hash`, MAX(`backupVersion`), `localExists`, `createdAtMs`
                    FROM `backup_file_records`
                    GROUP BY `sourceFileIdentity`
                    """.trimIndent()
                )

                // Backfill backup_versions from existing v1.3.0 verified backup_file_records
                db.execSQL(
                    """
                    INSERT OR IGNORE INTO `backup_versions` (
                        `accountId`, `sourceFileIdentity`, `versionNumber`, `driveFileId`,
                        `driveFolderId`, `driveStoredName`, `relativePath`, `fileSize`,
                        `sha256Hash`, `md5Hash`, `verificationStatus`, `preservedAfterLocalDelete`, `createdAtMs`
                    )
                    SELECT 'default_account', `sourceFileIdentity`, `backupVersion`, COALESCE(`driveFileId`, ''),
                           `driveFolderId`, `driveStoredName`, `relativePath`, `fileSize`,
                           `sha256Hash`, `md5Hash`, `verificationStatus`,
                           CASE WHEN `localExists` = 0 THEN 1 ELSE 0 END,
                           COALESCE(`lastSuccessfulBackupMs`, `createdAtMs`)
                    FROM `backup_file_records`
                    WHERE `uploadStatus` = 'VERIFIED' AND `driveFileId` IS NOT NULL
                    """.trimIndent()
                )
            }
        }

        @Volatile
        private var INSTANCE: PermanentBackupDatabase? = null

        fun getInstance(context: Context): PermanentBackupDatabase {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: buildDatabase(context.applicationContext).also { INSTANCE = it }
            }
        }

        private fun buildDatabase(appContext: Context): PermanentBackupDatabase {
            return Room.databaseBuilder(
                appContext,
                PermanentBackupDatabase::class.java,
                DB_NAME
            )
                .setJournalMode(JournalMode.WRITE_AHEAD_LOGGING)
                .addMigrations(MIGRATION_1_2)
                .build()
        }
    }
}
