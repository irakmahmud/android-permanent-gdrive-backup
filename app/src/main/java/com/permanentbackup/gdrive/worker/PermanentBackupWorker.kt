package com.permanentbackup.gdrive.worker

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.os.Environment
import android.os.FileObserver
import android.provider.MediaStore
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
import com.permanentbackup.gdrive.domain.engine.AndroidStorageScanner
import com.permanentbackup.gdrive.domain.model.BackupSettings
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * WorkManager CoroutineWorker for periodic background backup, real-time MediaStore ContentUriTrigger
 * sync, network reconnection retry, and post-reboot recovery while respecting Doze Mode,
 * Battery Saver, and Android 16 Job Quotas.
 */
class PermanentBackupWorker(
    appContext: Context,
    params: WorkerParameters
) : CoroutineWorker(appContext, params) {

    override suspend fun doWork(): Result {
        val app = applicationContext as? PermanentBackupApp ?: return Result.failure()
        val currentSettings = app.repository.settings.value
        // Re-arm one-time ContentUriTrigger observer for continuous real-time MediaStore watching
        BackupWorkScheduler.scheduleContentUriTriggeredBackup(applicationContext, currentSettings)

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
    const val CONTENT_URI_TRIGGER_WORK_NAME = "permanent_gdrive_content_uri_trigger"

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
        scheduleContentUriTriggeredBackup(context, settings)
    }

    /**
     * Registers a WorkManager ContentUriTrigger on MediaStore Images, Video, and Audio so new or
     * modified media files trigger an automatic incremental backup within seconds.
     */
    fun scheduleContentUriTriggeredBackup(context: Context, settings: BackupSettings) {
        if (settings.backgroundSyncPaused || !settings.userBackupConsentGranted) return
        try {
            val networkType = if (settings.wifiOnly || settings.unmeteredNetworkOnly) {
                NetworkType.UNMETERED
            } else {
                NetworkType.CONNECTED
            }
            val constraints = Constraints.Builder()
                .setRequiredNetworkType(networkType)
                .setRequiresCharging(settings.chargingOnly)
                .setRequiresBatteryNotLow(true)
                .addContentUriTrigger(MediaStore.Images.Media.EXTERNAL_CONTENT_URI, true)
                .addContentUriTrigger(MediaStore.Video.Media.EXTERNAL_CONTENT_URI, true)
                .addContentUriTrigger(MediaStore.Audio.Media.EXTERNAL_CONTENT_URI, true)
                .setTriggerContentUpdateDelay(10, TimeUnit.SECONDS)
                .setTriggerContentMaxDelay(60, TimeUnit.SECONDS)
                .build()

            val request = OneTimeWorkRequestBuilder<PermanentBackupWorker>()
                .setConstraints(constraints)
                .build()

            WorkManager.getInstance(context).enqueueUniqueWork(
                CONTENT_URI_TRIGGER_WORK_NAME,
                ExistingWorkPolicy.REPLACE,
                request
            )
        } catch (_: Throwable) {
            // Safe fallback in unit test environments without MediaStore or WorkManager init
        }
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
 * Real-time inotify / FileObserver watcher for standard phone storage directories
 * (`DCIM`, `Pictures`, `Movies`, `Documents`, `Download`, `Music`, `WhatsApp`, `Telegram`).
 * Only watches `CLOSE_WRITE`, `MOVED_TO`, and `CREATE` events — NEVER reacts to `DELETE` events
 * with any remote deletion (strictly preserving the Zero-Deletion invariant).
 */
class LocalStorageFileObserverManager(
    private val onNewOrModifiedFileDetected: (String) -> Unit
) {
    private val activeObservers = mutableListOf<FileObserver>()
    private val watchedPaths = mutableSetOf<String>()
    private val lastTriggerTimeMs = AtomicLong(0L)

    companion object {
        private val EXCLUDED_DIR_NAMES = setOf("restored_from_drive", "drive_mirror")
        private const val MAX_WATCH_DEPTH = 3

        fun isExcludedRelativePath(relPath: String): Boolean {
            val segments = relPath.replace('\\', '/').lowercase().split('/')
            return segments.any { it.startsWith(".") || it in EXCLUDED_DIR_NAMES }
        }
    }

    @Synchronized
    fun getWatchedDirectoryPaths(): Set<String> = watchedPaths.toSet()

    @Suppress("DEPRECATION")
    @Synchronized
    fun startWatching(customRootDirs: List<File>? = null) {
        stopWatching()
        val roots = customRootDirs ?: try {
            listOf(Environment.getExternalStorageDirectory())
        } catch (_: Throwable) {
            emptyList()
        }

        val eventMask = FileObserver.CLOSE_WRITE or FileObserver.MOVED_TO or FileObserver.CREATE
        for (root in roots) {
            if (!root.exists() || !root.isDirectory) continue
            for ((folderName, _) in AndroidStorageScanner.TARGET_STANDARD_FOLDERS) {
                val dir = File(root, folderName)
                if (dir.exists() && dir.isDirectory) {
                    watchDirectoryTreeRecursive(
                        dir = dir,
                        relativePrefix = folderName.trim('/'),
                        currentDepth = 0,
                        eventMask = eventMask
                    )
                }
            }
        }
    }

    @Suppress("DEPRECATION")
    @Synchronized
    private fun watchDirectoryTreeRecursive(
        dir: File,
        relativePrefix: String,
        currentDepth: Int,
        eventMask: Int
    ) {
        if (currentDepth > MAX_WATCH_DEPTH) return
        if (!dir.exists() || !dir.isDirectory || !dir.canRead()) return
        if (isExcludedRelativePath(relativePrefix)) return
        if (!watchedPaths.add(relativePrefix)) return

        val observer = object : FileObserver(dir.absolutePath, eventMask) {
            override fun onEvent(event: Int, path: String?) {
                if (path.isNullOrBlank() || path.startsWith(".")) return
                val childRelPath = "$relativePrefix/$path"
                if (isExcludedRelativePath(childRelPath)) return

                val childFile = File(dir, path)
                if ((event and FileObserver.CREATE) != 0 && childFile.isDirectory) {
                    watchDirectoryTreeRecursive(
                        dir = childFile,
                        relativePrefix = childRelPath,
                        currentDepth = currentDepth + 1,
                        eventMask = eventMask
                    )
                    return
                }

                val now = System.currentTimeMillis()
                val prev = lastTriggerTimeMs.get()
                if (now - prev >= 3_000L && lastTriggerTimeMs.compareAndSet(prev, now)) {
                    onNewOrModifiedFileDetected(childRelPath)
                }
            }
        }
        try {
            observer.startWatching()
            activeObservers.add(observer)
        } catch (_: Throwable) {
        }

        if (currentDepth < MAX_WATCH_DEPTH) {
            val subDirs = dir.listFiles { f ->
                f.isDirectory && !f.name.startsWith(".") && f.name.lowercase() !in EXCLUDED_DIR_NAMES
            }.orEmpty()
            for (sub in subDirs) {
                watchDirectoryTreeRecursive(
                    dir = sub,
                    relativePrefix = "$relativePrefix/${sub.name}",
                    currentDepth = currentDepth + 1,
                    eventMask = eventMask
                )
            }
        }
    }

    @Synchronized
    fun stopWatching() {
        for (obs in activeObservers) {
            try {
                obs.stopWatching()
            } catch (_: Throwable) {
            }
        }
        activeObservers.clear()
        watchedPaths.clear()
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
