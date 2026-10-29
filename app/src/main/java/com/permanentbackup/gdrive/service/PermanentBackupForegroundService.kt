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
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.R
import com.permanentbackup.gdrive.domain.model.ActiveUploadProgress
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.launch

/**
 * Foreground Service for long-running streaming backups with Windows Desktop-style live system tray
 * progress notification (active file name, determinate progress bar %, speed, ETA, and Pause/Resume/Stop actions).
 * Complies with Android 14/15/16 ForegroundServiceType DATA_SYNC and Job/FGS timeout quotas.
 */
class PermanentBackupForegroundService : Service() {

    companion object {
        const val CHANNEL_ID = "permanent_gdrive_backup_channel"
        const val NOTIFICATION_ID = 4101
        const val ACTION_START_BACKUP = "com.permanentbackup.gdrive.action.START_BACKUP"
        const val ACTION_PAUSE_BACKUP = "com.permanentbackup.gdrive.action.PAUSE_BACKUP"
        const val ACTION_RESUME_BACKUP = "com.permanentbackup.gdrive.action.RESUME_BACKUP"
        const val ACTION_STOP_BACKUP = "com.permanentbackup.gdrive.action.STOP_BACKUP"

        fun startManualBackupService(context: Context) {
            val intent = Intent(context, PermanentBackupForegroundService::class.java).apply {
                action = ACTION_START_BACKUP
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }
    }

    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var progressObserverJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        ensureNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val app = application as? PermanentBackupApp

        when (intent?.action) {
            ACTION_STOP_BACKUP -> {
                app?.repository?.setBackupPaused(true)
                stopSelf()
                return START_NOT_STICKY
            }
            ACTION_PAUSE_BACKUP -> {
                app?.repository?.setBackupPaused(true)
                updateNotification(null, isPaused = true)
                return START_NOT_STICKY
            }
            ACTION_RESUME_BACKUP -> {
                app?.repository?.setBackupPaused(false)
            }
        }

        val initialNotification = buildProgressNotification(
            title = getString(R.string.app_name),
            contentText = "Scanning & syncing to Google Drive (Zero-Delete Policy)...",
            progressPercent = null,
            isPaused = app?.repository?.backupEngine?.isPaused?.value == true
        )
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(
                NOTIFICATION_ID,
                initialNotification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
            )
        } else {
            startForeground(NOTIFICATION_ID, initialNotification)
        }

        if (app != null) {
            startObservingLiveProgress(app)
            serviceScope.launch {
                try {
                    app.repository.runBackupNow()
                } finally {
                    progressObserverJob?.cancel()
                    stopSelf(startId)
                }
            }
        } else {
            stopSelf(startId)
        }

        return START_REDELIVER_INTENT
    }

    private fun startObservingLiveProgress(app: PermanentBackupApp) {
        progressObserverJob?.cancel()
        progressObserverJob = serviceScope.launch {
            combine(
                app.repository.backupEngine.activeUploadProgress,
                app.repository.backupEngine.isPaused
            ) { progress, paused ->
                progress to paused
            }.collect { (progress, paused) ->
                updateNotification(progress, paused)
            }
        }
    }

    private fun updateNotification(progress: ActiveUploadProgress?, isPaused: Boolean) {
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager ?: return
        val notification = if (isPaused) {
            buildProgressNotification(
                title = "Permanent Drive — Paused",
                contentText = "Resumable session preserved; tap Resume to continue.",
                progressPercent = progress?.fraction?.let { (it * 100f).toInt().coerceIn(0, 100) },
                isPaused = true
            )
        } else if (progress != null) {
            val pct = (progress.fraction * 100f).toInt().coerceIn(0, 100)
            val speedStr = formatSpeed(progress.transferSpeedBps)
            val etaStr = progress.estimatedRemainingSeconds?.let { " • ETA ${it}s" }.orEmpty()
            buildProgressNotification(
                title = "Uploading ${progress.fileName} (v${progress.backupVersion})",
                contentText = "$pct% • $speedStr$etaStr",
                progressPercent = pct,
                isPaused = false
            )
        } else {
            buildProgressNotification(
                title = getString(R.string.app_name),
                contentText = "Streaming Upload-Only Backup Active (Never-Delete Policy)",
                progressPercent = null,
                isPaused = false
            )
        }
        manager.notify(NOTIFICATION_ID, notification)
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

    /**
     * Handles Android 15/16 dataSync Foreground Service 6-hour quota timeout gracefully:
     * saves current resumable progress in Room and delegates continuation to WorkManager.
     */
    override fun onTimeout(startId: Int, fgsType: Int) {
        super.onTimeout(startId, fgsType)
        BackupWorkScheduler.enqueueImmediateRetryWork(applicationContext)
        stopSelf(startId)
    }

    override fun onDestroy() {
        progressObserverJob?.cancel()
        serviceScope.cancel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun ensureNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val manager = getSystemService(NotificationManager::class.java)
            val channel = NotificationChannel(
                CHANNEL_ID,
                getString(R.string.notification_channel_name),
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = getString(R.string.notification_channel_desc)
            }
            manager?.createNotificationChannel(channel)
        }
    }

    private fun buildProgressNotification(
        title: String,
        contentText: String,
        progressPercent: Int?,
        isPaused: Boolean
    ): Notification {
        val pendingFlags = PendingIntent.FLAG_UPDATE_CURRENT or
            (if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) PendingIntent.FLAG_IMMUTABLE else 0)

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
        } else if (!isPaused) {
            builder.setProgress(100, 0, true)
        }
        return builder.build()
    }
}
