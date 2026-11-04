package com.permanentbackup.gdrive.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.permanentbackup.gdrive.MainActivity
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.R
import com.permanentbackup.gdrive.domain.engine.PermanentBackupEngine
import com.permanentbackup.gdrive.domain.model.ActiveUploadProgress
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import com.permanentbackup.gdrive.worker.LocalStorageFileObserverManager
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/**
 * 24/7 Continuous Background Auto-Backup Foreground Service:
 * - Never stops in the background after a scan cycle finishes (uses START_STICKY + continuous watcher loop).
 * - Watches local phone folders via FileObserver even when MainActivity is closed.
 * - Shows exact Phone Source Folder/File and Google Drive Destination Folder/File in live notifications.
 * - Complies with Android 14/15/16 ForegroundServiceType DATA_SYNC and Job/FGS timeout quotas.
 */
class PermanentBackupForegroundService : Service() {

    companion object {
        const val CHANNEL_ID = "permanent_gdrive_backup_channel"
        const val NOTIFICATION_ID = 4101
        const val ACTION_START_BACKUP = "com.permanentbackup.gdrive.action.START_BACKUP"
        const val ACTION_PAUSE_BACKUP = "com.permanentbackup.gdrive.action.PAUSE_BACKUP"
        const val ACTION_RESUME_BACKUP = "com.permanentbackup.gdrive.action.RESUME_BACKUP"
        const val ACTION_STOP_BACKUP = "com.permanentbackup.gdrive.action.STOP_BACKUP"
        /** Upload already-queued files only (share-sheet / "Upload File") — no full storage rescan. */
        const val ACTION_UPLOAD_QUEUE = "com.permanentbackup.gdrive.action.UPLOAD_QUEUE"

        private const val CONTINUOUS_AUTO_BACKUP_INTERVAL_MS = 30_000L

        @Volatile
        private var activeInstance: PermanentBackupForegroundService? = null

        fun wakeUpIfRunning(uploadQueueOnly: Boolean = false): Boolean {
            val instance = activeInstance ?: return false
            instance.wakeUp(uploadQueueOnly)
            return true
        }

        fun startManualBackupService(context: Context) {
            try {
                if (wakeUpIfRunning(uploadQueueOnly = false)) {
                    return
                }
                val appContext = context.applicationContext ?: context
                val intent = Intent(appContext, PermanentBackupForegroundService::class.java).apply {
                    action = ACTION_START_BACKUP
                }
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    appContext.startForegroundService(intent)
                } else {
                    appContext.startService(intent)
                }
            } catch (_: Throwable) {
                BackupWorkScheduler.enqueueImmediateRetryWork(context)
            }
        }

        fun startUploadQueueService(context: Context) {
            try {
                if (wakeUpIfRunning(uploadQueueOnly = true)) {
                    return
                }
                val appContext = context.applicationContext ?: context
                val intent = Intent(appContext, PermanentBackupForegroundService::class.java).apply {
                    action = ACTION_UPLOAD_QUEUE
                }
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    appContext.startForegroundService(intent)
                } else {
                    appContext.startService(intent)
                }
            } catch (_: Throwable) {
                BackupWorkScheduler.enqueueImmediateRetryWork(context)
            }
        }
    }

    private val serviceExceptionHandler = CoroutineExceptionHandler { _, _ -> }
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO + serviceExceptionHandler)
    private var progressObserverJob: Job? = null
    private var continuousLoopJob: Job? = null
    private val wakeUpChannel = Channel<Boolean>(Channel.CONFLATED)

    private val serviceFileObserver = LocalStorageFileObserverManager { _ ->
        wakeUpChannel.trySend(false)
    }

    fun wakeUp(uploadQueueOnly: Boolean = false) {
        try {
            serviceFileObserver.startWatching()
        } catch (_: Throwable) {
        }
        wakeUpChannel.trySend(uploadQueueOnly)
    }

    override fun onCreate() {
        super.onCreate()
        activeInstance = this
        ensureNotificationChannel()
        try {
            serviceFileObserver.startWatching()
        } catch (_: Throwable) {
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        activeInstance = this
        val app = application as? PermanentBackupApp

        // Always promote to foreground first to satisfy Android O+ 5-second startForeground contract
        val initialNotification = buildProgressNotification(
            title = "Auto-Backup Active (24/7 Never-Stop)",
            contentText = "Phone Folders → Google Drive: My Drive/${PermanentBackupEngine.ROOT_DRIVE_FOLDER_NAME}/",
            bigText = "From Phone: /storage/emulated/0/{DCIM, Pictures, Videos, Downloads, Documents, Media}\nTo Google Drive: My Drive / ${PermanentBackupEngine.ROOT_DRIVE_FOLDER_NAME} / <Phone_Folder>",
            progressPercent = null,
            isPaused = app?.repository?.backupEngine?.isPaused?.value == true,
            indeterminate = false
        )
        val promoted = try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(
                    NOTIFICATION_ID,
                    initialNotification,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
                )
            } else {
                startForeground(NOTIFICATION_ID, initialNotification)
            }
            true
        } catch (_: Throwable) {
            try {
                startForeground(NOTIFICATION_ID, initialNotification)
                true
            } catch (_: Throwable) {
                false
            }
        }

        if (!promoted) {
            activeInstance = null
            runCatching { BackupWorkScheduler.enqueueImmediateRetryWork(applicationContext) }
            stopSelf()
            return START_NOT_STICKY
        }

        when (intent?.action) {
            ACTION_STOP_BACKUP -> {
                app?.repository?.setBackupPaused(true)
                serviceFileObserver.stopWatching()
                continuousLoopJob?.cancel()
                progressObserverJob?.cancel()
                activeInstance = null
                stopSelf()
                return START_NOT_STICKY
            }
            ACTION_PAUSE_BACKUP -> {
                app?.repository?.setBackupPaused(true)
                updateNotification(null, isPaused = true, scanStatus = null)
                return START_STICKY
            }
            ACTION_RESUME_BACKUP -> {
                app?.repository?.setBackupPaused(false)
            }
        }

        if (app != null) {
            startObservingLiveProgress(app)
            val uploadQueueOnly = intent?.action == ACTION_UPLOAD_QUEUE
            wakeUpChannel.trySend(uploadQueueOnly)
            ensureContinuousAutoBackupLoopRunning(app)
        }

        return START_STICKY
    }

    private fun ensureContinuousAutoBackupLoopRunning(app: PermanentBackupApp) {
        if (continuousLoopJob?.isActive == true) return
        continuousLoopJob = serviceScope.launch {
            runCatching { app.repository.syncPersistedAccountOnStartup() }
            runCatching { serviceFileObserver.startWatching() }

            var nextUploadQueueOnly = wakeUpChannel.tryReceive().getOrNull() ?: false
            while (isActive) {
                val uploadQueueOnly = nextUploadQueueOnly
                nextUploadQueueOnly = false
                try {
                    if (!app.repository.authManager.accountState.value.isConnected) {
                        runCatching { app.repository.syncPersistedAccountOnStartup() }
                    }
                    if (serviceFileObserver.getWatchedDirectoryPaths().isEmpty() &&
                        app.repository.storageSource.hasStoragePermission
                    ) {
                        runCatching { serviceFileObserver.startWatching() }
                    }
                    if (!app.repository.settings.value.backgroundSyncPaused &&
                        !app.repository.backupEngine.isPaused.value
                    ) {
                        if (uploadQueueOnly) {
                            app.repository.runUploadQueueNow()
                        } else {
                            app.repository.runBackupNow(respectRetryBackoff = false)
                        }
                    }
                } catch (_: Throwable) {
                }

                // Wait for next real-time FileObserver trigger, manual trigger, or 30s heartbeat scan
                val wakeSignal = withTimeoutOrNull(CONTINUOUS_AUTO_BACKUP_INTERVAL_MS) {
                    wakeUpChannel.receive()
                }
                nextUploadQueueOnly = wakeSignal ?: false
            }
        }
    }

    private fun startObservingLiveProgress(app: PermanentBackupApp) {
        if (progressObserverJob?.isActive == true) return
        progressObserverJob = serviceScope.launch {
            combine(
                app.repository.backupEngine.activeUploadProgress,
                app.repository.backupEngine.isPaused,
                app.repository.backupEngine.currentScanActivity
            ) { progress, paused, scanStatus ->
                Triple(progress, paused, scanStatus)
            }.collect { (progress, paused, scanStatus) ->
                updateNotification(progress, paused, scanStatus)
            }
        }
    }

    private fun updateNotification(
        progress: ActiveUploadProgress?,
        isPaused: Boolean,
        scanStatus: String?
    ) {
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager ?: return
        val rootDriveName = PermanentBackupEngine.ROOT_DRIVE_FOLDER_NAME
        val notification = if (isPaused) {
            buildProgressNotification(
                title = "Permanent Drive — Auto-Backup Paused",
                contentText = "Tap Resume to continue backing up to Google Drive ($rootDriveName).",
                bigText = "Resumable byte offset preserved.\nGoogle Drive Target: My Drive / $rootDriveName",
                progressPercent = progress?.fraction?.let { (it * 100f).toInt().coerceIn(0, 100) },
                isPaused = true,
                indeterminate = false
            )
        } else if (progress != null) {
            val pct = (progress.fraction * 100f).toInt().coerceIn(0, 100)
            val speedStr = formatSpeed(progress.transferSpeedBps)
            val etaStr = progress.estimatedRemainingSeconds?.let { " • ETA ${it}s" }.orEmpty()
            val phonePath = "/storage/emulated/0/${progress.relativePath}"
            val driveSubFolder = progress.relativePath.substringBeforeLast('/', "")
            val drivePath = if (driveSubFolder.isBlank()) {
                "My Drive/$rootDriveName/${progress.fileName}"
            } else {
                "My Drive/$rootDriveName/$driveSubFolder/${progress.fileName}"
            }
            buildProgressNotification(
                title = "Backing up ${progress.fileName} ($pct% • $speedStr$etaStr)",
                contentText = "Phone: /${progress.relativePath} → Drive: $rootDriveName/${progress.relativePath}",
                bigText = "From Phone: $phonePath\nTo Google Drive: $drivePath (v${progress.backupVersion})\nProgress: $pct% • $speedStr$etaStr",
                progressPercent = pct,
                isPaused = false,
                indeterminate = false
            )
        } else {
            val activeDetail = scanStatus
                ?: "From Phone: /storage/emulated/0/{DCIM, Pictures, Videos, Downloads, Documents, Media}\nTo Google Drive: My Drive / $rootDriveName / <Phone_Folder>"
            buildProgressNotification(
                title = "Auto-Backup Active (24/7 Background Sync)",
                contentText = "Phone → Google Drive: My Drive/$rootDriveName/",
                bigText = activeDetail,
                progressPercent = null,
                isPaused = false,
                indeterminate = false
            )
        }
        try {
            manager.notify(NOTIFICATION_ID, notification)
        } catch (_: Throwable) {
        }
    }

    private fun formatSpeed(bps: Long): String {
        if (bps <= 0L) return "Calculating speed..."
        val kbps = bps / 1024.0
        return if (kbps >= 1024.0) {
            String.format("%.1f MB/s", kbps / 1024.0)
        } else {
            String.format("%.0f KB/s", kbps)
        }
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        super.onTaskRemoved(rootIntent)
        try {
            BackupWorkScheduler.enqueueImmediateRetryWork(applicationContext)
            startManualBackupService(applicationContext)
        } catch (_: Throwable) {
        }
    }

    /**
     * Handles Android 15/16 dataSync Foreground Service 6-hour quota timeout gracefully:
     * saves current resumable progress in Room and delegates continuation to WorkManager.
     */
    override fun onTimeout(startId: Int, fgsType: Int) {
        super.onTimeout(startId, fgsType)
        activeInstance = null
        BackupWorkScheduler.enqueueImmediateRetryWork(applicationContext)
        stopSelf(startId)
    }

    override fun onDestroy() {
        activeInstance = null
        serviceFileObserver.stopWatching()
        continuousLoopJob?.cancel()
        progressObserverJob?.cancel()
        serviceScope.cancel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun ensureNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            try {
                val manager = getSystemService(NotificationManager::class.java)
                val channel = NotificationChannel(
                    CHANNEL_ID,
                    getString(R.string.notification_channel_name),
                    NotificationManager.IMPORTANCE_LOW
                ).apply {
                    description = getString(R.string.notification_channel_desc)
                }
                manager?.createNotificationChannel(channel)
            } catch (_: Throwable) {
            }
        }
    }

    private fun buildProgressNotification(
        title: String,
        contentText: String,
        bigText: String,
        progressPercent: Int?,
        isPaused: Boolean,
        indeterminate: Boolean
    ): Notification {
        val pendingFlags = PendingIntent.FLAG_UPDATE_CURRENT or
            (if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) PendingIntent.FLAG_IMMUTABLE else 0)

        val openAppIntent = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        val openAppPending = PendingIntent.getActivity(this, 100, openAppIntent, pendingFlags)

        val pauseResumeIntent = Intent(this, PermanentBackupForegroundService::class.java).apply {
            action = if (isPaused) ACTION_RESUME_BACKUP else ACTION_PAUSE_BACKUP
        }
        val pauseResumePending = PendingIntent.getService(this, 101, pauseResumeIntent, pendingFlags)

        val stopIntent = Intent(this, PermanentBackupForegroundService::class.java).apply {
            action = ACTION_STOP_BACKUP
        }
        val stopPending = PendingIntent.getService(this, 102, stopIntent, pendingFlags)

        val builder = NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle(title)
            .setContentText(contentText)
            .setStyle(NotificationCompat.BigTextStyle().bigText(bigText))
            .setContentIntent(openAppPending)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .addAction(
                android.R.drawable.ic_media_pause,
                if (isPaused) "Resume" else "Pause",
                pauseResumePending
            )
            .addAction(
                android.R.drawable.ic_menu_close_clear_cancel,
                "Stop",
                stopPending
            )

        if (progressPercent != null) {
            builder.setProgress(100, progressPercent, false)
        } else if (indeterminate && !isPaused) {
            builder.setProgress(100, 0, true)
        }
        return builder.build()
    }
}
