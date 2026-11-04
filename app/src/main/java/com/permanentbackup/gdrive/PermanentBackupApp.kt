package com.permanentbackup.gdrive

import android.app.Application
import android.os.Looper
import com.permanentbackup.gdrive.data.local.PermanentBackupDatabase
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.data.repository.BackupRepository
import com.permanentbackup.gdrive.domain.engine.AndroidStorageScanner
import com.permanentbackup.gdrive.service.PermanentBackupForegroundService
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

class PermanentBackupApp : Application() {

    private val appScope = CoroutineScope(
        SupervisorJob() + Dispatchers.IO + CoroutineExceptionHandler { _, _ -> }
    )

    val database: PermanentBackupDatabase by lazy(LazyThreadSafetyMode.SYNCHRONIZED) {
        PermanentBackupDatabase.getInstance(this)
    }

    val repository: BackupRepository by lazy(LazyThreadSafetyMode.SYNCHRONIZED) {
        val storageScanner = AndroidStorageScanner(this)
        val authManager = GoogleAuthManager(this)
        val driveApiClient = DriveApiClient()

        BackupRepository(
            context = this,
            backupDao = database.backupDao(),
            storageSource = storageScanner,
            driveApiClient = driveApiClient,
            authManager = authManager
        )
    }

    override fun onCreate() {
        super.onCreate()
        installGlobalCrashShield()
        appScope.launch {
            runCatching { repository.syncPersistedAccountOnStartup() }
            runCatching { BackupWorkScheduler.schedulePeriodicBackup(this@PermanentBackupApp, repository.settings.value) }
            runCatching { BackupWorkScheduler.enqueueImmediateRetryWork(this@PermanentBackupApp, repository.settings.value) }
            if (isProcessInForeground()) {
                runCatching { PermanentBackupForegroundService.startManualBackupService(this@PermanentBackupApp) }
            }
        }
    }

    fun isProcessInForeground(): Boolean {
        return try {
            val appProcessInfo = android.app.ActivityManager.RunningAppProcessInfo()
            android.app.ActivityManager.getMyMemoryState(appProcessInfo)
            appProcessInfo.importance <= android.app.ActivityManager.RunningAppProcessInfo.IMPORTANCE_FOREGROUND_SERVICE
        } catch (_: Throwable) {
            false
        }
    }

    /**
     * Global crash shield ensuring the app never crashes on any click or background operation:
     * - Catches uncaught exceptions on background threads without terminating the process.
     * - Recovers the Android Main Looper if an unexpected transient UI exception ever reaches the top level,
     *   with rate-limiting so corrupted Choreographer frames never cause a 100% CPU freeze.
     */
    private fun installGlobalCrashShield() {
        try {
            val previousHandler = Thread.getDefaultUncaughtExceptionHandler()
            Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
                val isMainThread = try {
                    Looper.getMainLooper()?.thread == thread
                } catch (_: Throwable) {
                    false
                }
                if (isMainThread) {
                    var consecutiveFastCrashes = 0
                    var lastCrashMs = System.currentTimeMillis()
                    while (true) {
                        try {
                            Looper.loop()
                        } catch (inner: Throwable) {
                            if (inner is OutOfMemoryError) {
                                previousHandler?.uncaughtException(thread, inner)
                                break
                            }
                            val now = System.currentTimeMillis()
                            if (now - lastCrashMs < 300L) {
                                consecutiveFastCrashes++
                                if (consecutiveFastCrashes > 5) {
                                    try {
                                        Thread.sleep(500L)
                                    } catch (_: Throwable) {
                                    }
                                    consecutiveFastCrashes = 0
                                }
                            } else {
                                consecutiveFastCrashes = 0
                            }
                            lastCrashMs = now
                        }
                    }
                } else {
                    // Swallow non-fatal background thread exceptions so background sync never dies
                }
            }
        } catch (_: Throwable) {
        }
    }
}

