package com.permanentbackup.gdrive.ui.viewmodel

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.DashboardSummary
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.service.PermanentBackupForegroundService
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

class BackupViewModel(application: Application) : AndroidViewModel(application) {
    private val app = application as PermanentBackupApp
    private val repository = app.repository

    private val _dashboardSummary = MutableStateFlow(DashboardSummary())
    val dashboardSummary: StateFlow<DashboardSummary> = _dashboardSummary.asStateFlow()

    private val _folderCoverage = MutableStateFlow<List<FolderCoverageItem>>(emptyList())
    val folderCoverage: StateFlow<List<FolderCoverageItem>> = _folderCoverage.asStateFlow()

    val settings: StateFlow<BackupSettings> = repository.settings

    val activeQueue: StateFlow<List<BackupFileEntity>> = repository.observeActiveQueue()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val verifiedHistory: StateFlow<List<BackupFileEntity>> = repository.observeVerifiedHistory()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val failedUploads: StateFlow<List<BackupFileEntity>> = repository.observeFailedUploads()
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    init {
        refreshDashboardAndCoverage()
    }

    fun refreshDashboardAndCoverage() {
        viewModelScope.launch {
            _dashboardSummary.value = repository.buildDashboardSummary()
            _folderCoverage.value = repository.getFolderCoverageItems()
        }
    }

    fun triggerManualBackupNow() {
        PermanentBackupForegroundService.startManualBackupService(getApplication())
        viewModelScope.launch {
            repository.runBackupNow()
            refreshDashboardAndCoverage()
        }
    }

    fun retryFailedUploads() {
        viewModelScope.launch {
            repository.retryAllFailedUploads()
            refreshDashboardAndCoverage()
        }
    }

    fun retrySingleRecord(recordId: Long) {
        viewModelScope.launch {
            repository.requeueFailedRecord(recordId)
            repository.runBackupNow()
            refreshDashboardAndCoverage()
        }
    }

    fun updateSettings(newSettings: BackupSettings) {
        repository.updateSettings(newSettings)
        BackupWorkScheduler.schedulePeriodicBackup(getApplication(), newSettings)
        refreshDashboardAndCoverage()
    }

    fun connectGoogleAccount(email: String, displayName: String, accessToken: String, refreshToken: String?) {
        repository.authManager.onAccountAuthorized(
            email = email,
            displayName = displayName,
            accessToken = accessToken,
            refreshToken = refreshToken
        )
        refreshDashboardAndCoverage()
    }

    fun switchGoogleAccount(newEmail: String, newDisplayName: String, newAccessToken: String, newRefreshToken: String?) {
        repository.authManager.switchGoogleAccount(
            newEmail = newEmail,
            newDisplayName = newDisplayName,
            newAccessToken = newAccessToken,
            newRefreshToken = newRefreshToken
        )
        refreshDashboardAndCoverage()
    }

    fun disconnectGoogleAccount() {
        repository.authManager.disconnectAccount()
        refreshDashboardAndCoverage()
    }
}
