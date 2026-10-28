package com.permanentbackup.gdrive.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.R
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch

/**
 * Foreground Service for long-running streaming backups.
 * Complies with Android 14/15/16 ForegroundServiceType DATA_SYNC and Job/FGS timeout quotas.
 */
class PermanentBackupForegroundService : Service() {

    companion object {
        const val CHANNEL_ID = "permanent_gdrive_backup_channel"
        const val NOTIFICATION_ID = 4101
        const val ACTION_START_BACKUP = "com.permanentbackup.gdrive.action.START_BACKUP"
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

    override fun onCreate() {
        super.onCreate()
        ensureNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP_BACKUP) {
            stopSelf()
            return START_NOT_STICKY
        }

        val notification = buildProgressNotification("Streaming Upload-Only Backup Active (Never-Delete Policy)")
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(
                NOTIFICATION_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
            )
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }

        val app = application as? PermanentBackupApp
        if (app != null) {
            serviceScope.launch {
                try {
                    app.repository.runBackupNow()
                } finally {
                    stopSelf(startId)
                }
            }
        } else {
            stopSelf(startId)
        }

        return START_REDELIVER_INTENT
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

    private fun buildProgressNotification(contentText: String): Notification {
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle(getString(R.string.app_name))
            .setContentText(contentText)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .build()
    }
}
