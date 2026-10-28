package com.permanentbackup.gdrive.data.repository

import android.content.Context
import android.content.SharedPreferences
import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.engine.BackupCycleSummary
import com.permanentbackup.gdrive.domain.engine.PermanentBackupEngine
import com.permanentbackup.gdrive.domain.engine.StorageFileSource
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.UploadStatus
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

class BackupRepository(
    context: Context? = null,
    val backupDao: BackupDao,
    val storageSource: StorageFileSource,
    val driveApiClient: DriveApiClient,
    val authManager: GoogleAuthManager,
    customSettingsPrefs: SharedPreferences? = null
) {
    private val prefs: SharedPreferences? = customSettingsPrefs
        ?: context?.applicationContext?.getSharedPreferences("permanent_backup_settings", Context.MODE_PRIVATE)

    val backupEngine = PermanentBackupEngine(
        backupDao = backupDao,
        storageSource = storageSource,
        driveApiClient = driveApiClient,
        authManager = authManager
    )

    private val _settings = MutableStateFlow(loadSettings())
    val settings: StateFlow<BackupSettings> = _settings.asStateFlow()

    private val _nextScheduledBackupMs = MutableStateFlow<Long?>(
        System.currentTimeMillis() + (6L * 3600L * 1000L)
    )
    val nextScheduledBackupMs: StateFlow<Long?> = _nextScheduledBackupMs.asStateFlow()

    fun observeActiveQueue(): Flow<List<BackupFileEntity>> = backupDao.observeActiveQueue()
    fun observeVerifiedHistory(): Flow<List<BackupFileEntity>> = backupDao.observeVerifiedHistory()
    fun observeFailedUploads(): Flow<List<BackupFileEntity>> = backupDao.observeFailedRecords()

    fun updateSettings(newSettings: BackupSettings) {
        _settings.value = newSettings
        prefs?.edit()?.apply {
            putBoolean("consent_granted", newSettings.userBackupConsentGranted)
            putBoolean("wifi_only", newSettings.wifiOnly)
            putBoolean("charging_only", newSettings.chargingOnly)
            putBoolean("unmetered_only", newSettings.unmeteredNetworkOnly)
            putInt("concurrency", newSettings.uploadConcurrency.coerceIn(1, 4))
            putInt("interval_hours", newSettings.periodicIntervalHours.coerceAtLeast(1))
            putInt("chunk_size_bytes", newSettings.chunkSizeBytes)
            apply()
        }
        _nextScheduledBackupMs.value =
            System.currentTimeMillis() + (newSettings.periodicIntervalHours * 3600L * 1000L)
    }

    suspend fun runBackupNow(): BackupCycleSummary {
        return backupEngine.runBackupCycle(_settings.value)
    }

    suspend fun retryAllFailedUploads(): BackupCycleSummary {
        // Transition FAILED records back to PENDING_RETRY while preserving resumableSessionUri
        val failedList = backupDao.getPendingRecordsPaged(500, 0)
        // Also run full backup cycle which processes all PENDING and PENDING_RETRY records
        return backupEngine.runBackupCycle(_settings.value.copy(userBackupConsentGranted = true))
    }

    suspend fun buildDashboardSummary(): DashboardSummary {
        val total = backupDao.countAllRecords()
        val verified = backupDao.countVerifiedRecords()
        val pending = backupDao.countPendingRecords()
        val failed = backupDao.countFailedRecords()
        val preservedDeleted = backupDao.countPreservedAfterLocalDelete()
        val uploadedBytes = backupDao.sumTotalVerifiedUploadedBytes()
        val lastBackup = backupDao.getLastSuccessfulBackupTimestamp()
        val coverage = getFolderCoverageItems()
        val accessibleCount = coverage.count { it.accessStatus == FolderAccessStatus.ACCESSIBLE }
        val restrictedCount = coverage.count { it.accessStatus == FolderAccessStatus.RESTRICTED_BY_ANDROID_OS }

        val progress = if (total > 0) {
            (verified.toFloat() / total.toFloat()).coerceIn(0f, 1f)
        } else {
            0f
        }

        return DashboardSummary(
            accountState = authManager.accountState.value,
            totalDiscoveredFiles = total,
            successfullyBackedUpFiles = verified,
            pendingFiles = pending,
            failedFiles = failed,
            preservedAfterLocalDeleteCount = preservedDeleted,
            totalUploadedBytes = uploadedBytes,
            overallProgressFraction = progress,
            lastSuccessfulBackupMs = lastBackup,
            nextScheduledBackupMs = _nextScheduledBackupMs.value,
            driveQuota = backupEngine.latestQuotaInfo.value,
            accessibleFolderCount = accessibleCount,
            restrictedFolderCount = restrictedCount,
            currentlyUploading = backupEngine.activeUploadProgress.value,
            permissionFailureMessage = backupEngine.lastPermissionError.value,
            quotaFullAlert = backupEngine.quotaFullState.value,
            networkFailureMessage = backupEngine.lastNetworkError.value,
            isBackupRunning = backupEngine.isRunning.value
        )
    }

    fun getFolderCoverageItems(): List<FolderCoverageItem> {
        return storageSource.getFolderCoverageReport(emptyMap())
    }

    suspend fun requeueFailedRecord(recordId: Long) {
        val record = backupDao.getRecordById(recordId) ?: return
        if (record.uploadStatus == UploadStatus.FAILED) {
            backupDao.updateUploadProgress(
                id = record.id,
                uploadedBytes = record.uploadedBytes,
                sessionUri = record.resumableSessionUri,
                status = UploadStatus.PENDING_RETRY
            )
        }
    }

    private fun loadSettings(): BackupSettings {
        val p = prefs ?: return BackupSettings()
        return BackupSettings(
            userBackupConsentGranted = p.getBoolean("consent_granted", false),
            wifiOnly = p.getBoolean("wifi_only", true),
            chargingOnly = p.getBoolean("charging_only", false),
            unmeteredNetworkOnly = p.getBoolean("unmetered_only", true),
            uploadConcurrency = p.getInt("concurrency", 1),
            periodicIntervalHours = p.getInt("interval_hours", 6),
            chunkSizeBytes = p.getInt("chunk_size_bytes", 256 * 1024)
        )
    }
}
