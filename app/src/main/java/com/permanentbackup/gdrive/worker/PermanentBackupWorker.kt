package com.permanentbackup.gdrive.worker

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.domain.model.BackupSettings
import java.util.concurrent.TimeUnit

/**
 * WorkManager CoroutineWorker for periodic background backup, network reconnection retry,
 * and post-reboot recovery while respecting Doze Mode, Battery Saver, and Android 16 Job Quotas.
 */
class PermanentBackupWorker(
    appContext: Context,
    params: WorkerParameters
) : CoroutineWorker(appContext, params) {

    override suspend fun doWork(): Result {
        val app = applicationContext as? PermanentBackupApp ?: return Result.failure()
        val summary = app.repository.runBackupNow()

        if (summary.quotaFull || summary.permissionDenied) {
            // Do not endlessly spin retries when storage quota is full or user revoked permission
            return Result.failure()
        }

        return if (summary.failedUploads > 0) {
            Result.retry()
        } else {
            Result.success()
        }
    }
}

object BackupWorkScheduler {
    const val PERIODIC_BACKUP_WORK_NAME = "permanent_gdrive_periodic_backup"
    const val IMMEDIATE_RETRY_WORK_NAME = "permanent_gdrive_immediate_retry"

    fun buildWorkConstraints(settings: BackupSettings): Constraints {
        val networkType = if (settings.wifiOnly || settings.unmeteredNetworkOnly) {
            NetworkType.UNMETERED
        } else {
            NetworkType.CONNECTED
        }
        return Constraints.Builder()
            .setRequiredNetworkType(networkType)
            .setRequiresCharging(settings.chargingOnly)
            .setRequiresBatteryNotLow(true)
            .build()
    }

    fun schedulePeriodicBackup(context: Context, settings: BackupSettings) {
        val constraints = buildWorkConstraints(settings)
        val intervalHours = settings.periodicIntervalHours.toLong().coerceAtLeast(1L)

        val request = PeriodicWorkRequestBuilder<PermanentBackupWorker>(
            intervalHours,
            TimeUnit.HOURS
        )
            .setConstraints(constraints)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 15, TimeUnit.MINUTES)
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            PERIODIC_BACKUP_WORK_NAME,
            ExistingPeriodicWorkPolicy.UPDATE,
            request
        )
    }

    fun enqueueImmediateRetryWork(context: Context, settings: BackupSettings = BackupSettings()) {
        val constraints = buildWorkConstraints(settings)
        val request = OneTimeWorkRequestBuilder<PermanentBackupWorker>()
            .setConstraints(constraints)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 5, TimeUnit.MINUTES)
            .build()

        WorkManager.getInstance(context).enqueueUniqueWork(
            IMMEDIATE_RETRY_WORK_NAME,
            ExistingWorkPolicy.KEEP,
            request
        )
    }
}

/**
 * Recovers backup scheduling automatically after a phone restart or app update.
 */
class BootCompletedReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        val action = intent?.action ?: return
        if (action == Intent.ACTION_BOOT_COMPLETED ||
            action == Intent.ACTION_LOCKED_BOOT_COMPLETED ||
            action == Intent.ACTION_MY_PACKAGE_REPLACED
        ) {
            val app = context.applicationContext as? PermanentBackupApp
            val settings = app?.repository?.settings?.value ?: BackupSettings()
            BackupWorkScheduler.schedulePeriodicBackup(context, settings)
            BackupWorkScheduler.enqueueImmediateRetryWork(context, settings)
        }
    }
}
