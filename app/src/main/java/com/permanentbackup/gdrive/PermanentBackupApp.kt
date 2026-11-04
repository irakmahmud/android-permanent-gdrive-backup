package com.permanentbackup.gdrive

import android.app.Application
import com.permanentbackup.gdrive.data.local.PermanentBackupDatabase
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.data.repository.BackupRepository
import com.permanentbackup.gdrive.domain.engine.AndroidStorageScanner
import com.permanentbackup.gdrive.worker.BackupWorkScheduler

class PermanentBackupApp : Application() {

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
        try {
            BackupWorkScheduler.schedulePeriodicBackup(this, repository.settings.value)
        } catch (_: Throwable) {
        }
    }
}
