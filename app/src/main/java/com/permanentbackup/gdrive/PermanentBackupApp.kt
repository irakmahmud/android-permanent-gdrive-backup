package com.permanentbackup.gdrive

import android.app.Application
import com.permanentbackup.gdrive.data.local.PermanentBackupDatabase
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.data.repository.BackupRepository
import com.permanentbackup.gdrive.domain.engine.AndroidStorageScanner
import com.permanentbackup.gdrive.worker.BackupWorkScheduler

class PermanentBackupApp : Application() {

    lateinit var database: PermanentBackupDatabase
        private set

    lateinit var repository: BackupRepository
        private set

    override fun onCreate() {
        super.onCreate()
        database = PermanentBackupDatabase.getInstance(this)
        val storageScanner = AndroidStorageScanner(this)
        val authManager = GoogleAuthManager(this)
        val driveApiClient = DriveApiClient()

        repository = BackupRepository(
            context = this,
            backupDao = database.backupDao(),
            storageSource = storageScanner,
            driveApiClient = driveApiClient,
            authManager = authManager
        )

        BackupWorkScheduler.schedulePeriodicBackup(this, repository.settings.value)
    }
}
