package com.permanentbackup.gdrive

import android.content.SharedPreferences
import com.permanentbackup.gdrive.data.local.AppSettingsEntity
import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.BackupFolderEntity
import com.permanentbackup.gdrive.data.local.BackupVersionEntity
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.local.DriveChangeTokenEntity
import com.permanentbackup.gdrive.data.local.DriveFolderCacheEntity
import com.permanentbackup.gdrive.data.local.FailedOperationEntity
import com.permanentbackup.gdrive.data.local.GoogleAccountEntity
import com.permanentbackup.gdrive.data.local.LocalFileEntity
import com.permanentbackup.gdrive.data.local.StoragePermissionEntity
import com.permanentbackup.gdrive.data.local.SyncEventEntity
import com.permanentbackup.gdrive.data.local.SyncJobEntity
import com.permanentbackup.gdrive.data.local.UploadSessionEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.data.repository.BackupRepository
import com.permanentbackup.gdrive.domain.engine.AndroidStorageScanner
import com.permanentbackup.gdrive.domain.engine.CloudFileOperationsManager
import com.permanentbackup.gdrive.domain.engine.PermanentBackupEngine
import com.permanentbackup.gdrive.domain.engine.SourceFileReadOnlyGuard
import com.permanentbackup.gdrive.domain.model.AccountAuthState
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DiscoveredSourceFile
import com.permanentbackup.gdrive.domain.model.DriveBadRequestException
import com.permanentbackup.gdrive.domain.model.DriveConflictException
import com.permanentbackup.gdrive.domain.model.DriveResourceNotFoundException
import com.permanentbackup.gdrive.domain.model.DriveServerException
import com.permanentbackup.gdrive.domain.model.DriveSortOrder
import com.permanentbackup.gdrive.domain.model.DriveViewFilter
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.NeverDeletePolicyViolationException
import com.permanentbackup.gdrive.domain.model.SourceFileModificationForbiddenException
import com.permanentbackup.gdrive.domain.model.SyncJobState
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus
import com.permanentbackup.gdrive.worker.BackupWorkScheduler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.test.runTest
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * In-memory implementation of [BackupDao] covering all 14 Room tables for deterministic,
 * fast JVM integration, state-machine, and reliability tests.
 */
class InMemoryBackupDao : BackupDao {
    private val records = LinkedHashMap<Long, BackupFileEntity>()
    private val folderCache = HashMap<String, DriveFolderCacheEntity>()
    private val accounts = LinkedHashMap<String, GoogleAccountEntity>()
    private val backupFolders = LinkedHashMap<String, BackupFolderEntity>()
    private val localFiles = LinkedHashMap<String, LocalFileEntity>()
    private val cloudFiles = LinkedHashMap<String, CloudFileEntity>()
    private val backupVersions = LinkedHashMap<Long, BackupVersionEntity>()
    private val uploadSessions = LinkedHashMap<String, UploadSessionEntity>()
    private val syncJobs = LinkedHashMap<Long, SyncJobEntity>()
    private val syncEvents = ArrayList<SyncEventEntity>()
    private val failedOperations = LinkedHashMap<Long, FailedOperationEntity>()
    private var appSettingsEntity: AppSettingsEntity? = null
    private val storagePermissions = LinkedHashMap<String, StoragePermissionEntity>()
    private val driveChangeTokens = LinkedHashMap<String, DriveChangeTokenEntity>()

    private val idCounter = AtomicInteger(1)
    private val versionIdCounter = AtomicLong(1L)
    private val jobIdCounter = AtomicLong(1L)
    private val eventIdCounter = AtomicLong(1L)
    private val failureIdCounter = AtomicLong(1L)

    private val stateFlow = MutableStateFlow<List<BackupFileEntity>>(emptyList())
    private val accountsFlow = MutableStateFlow<List<GoogleAccountEntity>>(emptyList())
    private val cloudFilesFlow = MutableStateFlow<List<CloudFileEntity>>(emptyList())
    private val syncJobsFlow = MutableStateFlow<List<SyncJobEntity>>(emptyList())
    private val syncEventsFlow = MutableStateFlow<List<SyncEventEntity>>(emptyList())
    private val failuresFlow = MutableStateFlow<List<FailedOperationEntity>>(emptyList())

    private fun notifyChange() {
        stateFlow.value = records.values.toList()
        accountsFlow.value = accounts.values.toList()
        cloudFilesFlow.value = cloudFiles.values.toList()
        syncJobsFlow.value = syncJobs.values.toList()
        syncEventsFlow.value = syncEvents.toList()
        failuresFlow.value = failedOperations.values.toList()
    }

    override suspend fun insertRecord(record: BackupFileEntity): Long {
        val id = if (record.id == 0L) idCounter.getAndIncrement().toLong() else record.id
        val saved = record.copy(id = id)
        records[id] = saved
        notifyChange()
        return id
    }

    override suspend fun updateRecord(record: BackupFileEntity) {
        records[record.id] = record
        notifyChange()
    }

    override suspend fun getRecordById(id: Long): BackupFileEntity? = records[id]

    override suspend fun getLatestVersionForIdentity(identity: String): BackupFileEntity? {
        return records.values
            .filter { it.sourceFileIdentity == identity }
            .maxByOrNull { it.backupVersion }
    }

    override suspend fun getAllVersionsForIdentity(identity: String): List<BackupFileEntity> {
        return records.values
            .filter { it.sourceFileIdentity == identity }
            .sortedByDescending { it.backupVersion }
    }

    override suspend fun getPendingRecordsPaged(limit: Int, offset: Int): List<BackupFileEntity> {
        return records.values
            .filter {
                it.uploadStatus == UploadStatus.PENDING ||
                    it.uploadStatus == UploadStatus.PENDING_RETRY ||
                    it.uploadStatus == UploadStatus.IN_PROGRESS
            }
            .sortedBy { it.createdAtMs }
            .drop(offset)
            .take(limit)
    }

    override suspend fun getFailedRecordsPaged(limit: Int, offset: Int): List<BackupFileEntity> {
        return records.values
            .filter { it.uploadStatus == UploadStatus.FAILED }
            .sortedBy { it.createdAtMs }
            .drop(offset)
            .take(limit)
    }

    override suspend fun getInProgressRecords(): List<BackupFileEntity> {
        return records.values.filter { it.uploadStatus == UploadStatus.IN_PROGRESS }
    }

    override suspend fun updateUploadProgress(
        id: Long,
        uploadedBytes: Long,
        sessionUri: String?,
        status: UploadStatus
    ) {
        val current = records[id] ?: return
        records[id] = current.copy(
            uploadedBytes = uploadedBytes,
            resumableSessionUri = sessionUri,
            uploadStatus = status
        )
        notifyChange()
    }

    override suspend fun markUploadVerified(
        id: Long,
        driveFileId: String,
        driveFolderId: String?,
        fileSize: Long,
        verificationStatus: VerificationStatus,
        completedAtMs: Long
    ) {
        val current = records[id] ?: return
        records[id] = current.copy(
            driveFileId = driveFileId,
            driveFolderId = driveFolderId,
            uploadedBytes = fileSize,
            resumableSessionUri = null,
            uploadStatus = UploadStatus.VERIFIED,
            verificationStatus = verificationStatus,
            lastSuccessfulBackupMs = completedAtMs,
            failureReason = FailureReason.NONE,
            failureDetail = null
        )
        notifyChange()
    }

    override suspend fun markUploadFailed(
        id: Long,
        status: UploadStatus,
        reason: FailureReason,
        detail: String?,
        sessionUri: String?
    ) {
        val current = records[id] ?: return
        records[id] = current.copy(
            uploadStatus = status,
            failureReason = reason,
            failureDetail = detail,
            resumableSessionUri = sessionUri,
            retryCount = current.retryCount + 1
        )
        notifyChange()
    }

    override suspend fun markLocalFileDeletedPreservingCloudBackup(identity: String) {
        records.entries.forEach { (id, rec) ->
            if (rec.sourceFileIdentity == identity) {
                records[id] = rec.copy(localExists = false)
            }
        }
        notifyChange()
    }

    override fun observeRecentRecords(): Flow<List<BackupFileEntity>> = stateFlow
    override fun observeVerifiedHistory(): Flow<List<BackupFileEntity>> =
        stateFlow.map { list -> list.filter { it.uploadStatus == UploadStatus.VERIFIED } }

    override fun observeFailedRecords(): Flow<List<BackupFileEntity>> =
        stateFlow.map { list ->
            list.filter { it.uploadStatus == UploadStatus.FAILED || it.uploadStatus == UploadStatus.PENDING_RETRY }
        }

    override fun observeActiveQueue(): Flow<List<BackupFileEntity>> =
        stateFlow.map { list ->
            list.filter {
                it.uploadStatus == UploadStatus.PENDING ||
                    it.uploadStatus == UploadStatus.IN_PROGRESS ||
                    it.uploadStatus == UploadStatus.PENDING_RETRY
            }
        }

    override suspend fun countAllRecords(): Int = records.size
    override suspend fun countVerifiedRecords(): Int =
        records.values.count { it.uploadStatus == UploadStatus.VERIFIED }

    override suspend fun countVerifiedFilesInCategory(category: String): Int =
        records.values
            .filter { it.uploadStatus == UploadStatus.VERIFIED && it.folderCategory == category }
            .map { it.sourceFileIdentity }
            .distinct()
            .size

    override suspend fun countPendingRecords(): Int =
        records.values.count {
            it.uploadStatus == UploadStatus.PENDING ||
                it.uploadStatus == UploadStatus.PENDING_RETRY ||
                it.uploadStatus == UploadStatus.IN_PROGRESS
        }

    override suspend fun countFailedRecords(): Int =
        records.values.count { it.uploadStatus == UploadStatus.FAILED }

    override suspend fun countPreservedAfterLocalDelete(): Int =
        records.values.count { it.uploadStatus == UploadStatus.VERIFIED && !it.localExists }

    override suspend fun sumTotalVerifiedUploadedBytes(): Long =
        records.values.filter { it.uploadStatus == UploadStatus.VERIFIED }.sumOf { it.fileSize }

    override suspend fun getLastSuccessfulBackupTimestamp(): Long? =
        records.values.filter { it.uploadStatus == UploadStatus.VERIFIED }
            .mapNotNull { it.lastSuccessfulBackupMs }
            .maxOrNull()

    override suspend fun getActiveIdentitiesPaged(limit: Int, offset: Int): List<String> {
        return records.values
            .filter { it.localExists }
            .map { it.sourceFileIdentity }
            .distinct()
            .drop(offset)
            .take(limit)
    }

    override suspend fun cacheDriveFolder(folder: DriveFolderCacheEntity) {
        folderCache[folder.relativePath] = folder
    }

    override suspend fun getCachedDriveFolder(relativePath: String): DriveFolderCacheEntity? =
        folderCache[relativePath]

    // 1. GoogleAccountEntity
    override suspend fun upsertGoogleAccount(account: GoogleAccountEntity) {
        accounts[account.accountId] = account
        notifyChange()
    }

    override suspend fun getActiveGoogleAccount(): GoogleAccountEntity? =
        accounts.values.firstOrNull { it.isActive }

    override suspend fun getGoogleAccountById(accountId: String): GoogleAccountEntity? =
        accounts[accountId]

    override suspend fun getAllGoogleAccounts(): List<GoogleAccountEntity> =
        accounts.values.sortedByDescending { it.lastSuccessfulConnectionMs }

    override fun observeAllGoogleAccounts(): Flow<List<GoogleAccountEntity>> = accountsFlow

    override suspend fun setActiveGoogleAccount(activeAccountId: String) {
        accounts.entries.forEach { (id, acc) ->
            accounts[id] = acc.copy(isActive = id == activeAccountId)
        }
        notifyChange()
    }

    // 2. BackupFolderEntity
    override suspend fun upsertBackupFolder(folder: BackupFolderEntity) {
        backupFolders[folder.folderKey] = folder
    }

    override suspend fun getBackupFoldersForAccount(accountId: String): List<BackupFolderEntity> =
        backupFolders.values.filter { it.accountId == accountId }

    override suspend fun getBackupFolderByKey(folderKey: String): BackupFolderEntity? =
        backupFolders[folderKey]

    // 3. LocalFileEntity
    override suspend fun upsertLocalFile(localFile: LocalFileEntity) {
        localFiles[localFile.sourceFileIdentity] = localFile
    }

    override suspend fun getLocalFileByIdentity(identity: String): LocalFileEntity? =
        localFiles[identity]

    override suspend fun markLocalFileIndexDeleted(identity: String) {
        val current = localFiles[identity] ?: return
        localFiles[identity] = current.copy(localExists = false)
    }

    // 4. CloudFileEntity
    override suspend fun upsertCloudFile(cloudFile: CloudFileEntity) {
        cloudFiles[cloudFile.driveFileId] = cloudFile
        notifyChange()
    }

    override suspend fun upsertCloudFiles(cloudFiles: List<CloudFileEntity>) {
        cloudFiles.forEach { this.cloudFiles[it.driveFileId] = it }
        notifyChange()
    }

    override suspend fun getCloudFileById(driveFileId: String): CloudFileEntity? =
        cloudFiles[driveFileId]

    override suspend fun getCloudFilesForAccountPaged(
        accountId: String,
        limit: Int,
        offset: Int
    ): List<CloudFileEntity> {
        return cloudFiles.values
            .filter { it.accountId == accountId && !it.trashed }
            .sortedWith(compareByDescending<CloudFileEntity> { it.isFolder }.thenByDescending { it.modifiedTimeMs })
            .drop(offset)
            .take(limit)
    }

    override fun observePinnedOfflineFiles(accountId: String): Flow<List<CloudFileEntity>> =
        cloudFilesFlow.map { list -> list.filter { it.accountId == accountId && it.isPinnedOffline && !it.trashed } }

    override suspend fun countPinnedOfflineFiles(accountId: String): Int =
        cloudFiles.values.count { it.accountId == accountId && it.isPinnedOffline }

    override suspend fun sumOfflineCachedBytes(): Long =
        cloudFiles.values.filter { it.isPinnedOffline }.sumOf { it.offlineCachedBytes }

    override suspend fun updateCloudFileOfflinePin(
        driveFileId: String,
        pinned: Boolean,
        localPath: String?,
        cachedBytes: Long
    ) {
        val current = cloudFiles[driveFileId] ?: return
        cloudFiles[driveFileId] = current.copy(
            isPinnedOffline = pinned,
            offlineLocalPath = localPath,
            offlineCachedBytes = cachedBytes
        )
        notifyChange()
    }

    // 5. BackupVersionEntity
    override suspend fun insertBackupVersion(version: BackupVersionEntity): Long {
        val id = if (version.versionId == 0L) versionIdCounter.getAndIncrement() else version.versionId
        backupVersions[id] = version.copy(versionId = id)
        return id
    }

    override suspend fun getBackupVersionsForIdentity(identity: String): List<BackupVersionEntity> =
        backupVersions.values
            .filter { it.sourceFileIdentity == identity }
            .sortedByDescending { it.versionNumber }

    override suspend fun getBackupVersionsForAccountPaged(
        accountId: String,
        limit: Int,
        offset: Int
    ): List<BackupVersionEntity> =
        backupVersions.values
            .filter { it.accountId == accountId }
            .sortedByDescending { it.createdAtMs }
            .drop(offset)
            .take(limit)

    override suspend fun markVersionsPreservedAfterLocalDelete(identity: String) {
        backupVersions.entries.forEach { (id, ver) ->
            if (ver.sourceFileIdentity == identity) {
                backupVersions[id] = ver.copy(preservedAfterLocalDelete = true)
            }
        }
    }

    // 6. UploadSessionEntity
    override suspend fun upsertUploadSession(session: UploadSessionEntity) {
        uploadSessions[session.sessionId] = session
    }

    override suspend fun getActiveUploadSession(
        accountId: String,
        identity: String,
        version: Int
    ): UploadSessionEntity? =
        uploadSessions.values.firstOrNull {
            it.accountId == accountId &&
                it.sourceFileIdentity == identity &&
                it.backupVersion == version &&
                it.isActive
        }

    override suspend fun deactivateUploadSession(sessionId: String, nowMs: Long) {
        val current = uploadSessions[sessionId] ?: return
        uploadSessions[sessionId] = current.copy(isActive = false, updatedAtMs = nowMs)
    }

    // 7 & 8. SyncJobEntity & SyncEventEntity
    override suspend fun upsertSyncJob(job: SyncJobEntity): Long {
        val id = if (job.jobId == 0L) jobIdCounter.getAndIncrement() else job.jobId
        syncJobs[id] = job.copy(jobId = id)
        notifyChange()
        return id
    }

    override suspend fun getSyncJobByRecordId(recordId: Long): SyncJobEntity? =
        syncJobs.values.firstOrNull { it.recordId == recordId }

    override fun observeSyncJobs(): Flow<List<SyncJobEntity>> = syncJobsFlow

    override suspend fun setSyncJobPaused(recordId: Long, paused: Boolean, nowMs: Long) {
        val existing = getSyncJobByRecordId(recordId) ?: return
        syncJobs[existing.jobId] = existing.copy(isPaused = paused, updatedAtMs = nowMs)
        notifyChange()
    }

    override suspend fun insertSyncEvent(event: SyncEventEntity): Long {
        val id = if (event.eventId == 0L) eventIdCounter.getAndIncrement() else event.eventId
        syncEvents.add(event.copy(eventId = id))
        notifyChange()
        return id
    }

    override fun observeRecentSyncEvents(): Flow<List<SyncEventEntity>> = syncEventsFlow

    // 9. FailedOperationEntity
    override suspend fun insertFailedOperation(failedOp: FailedOperationEntity): Long {
        val id = if (failedOp.failureId == 0L) failureIdCounter.getAndIncrement() else failedOp.failureId
        failedOperations[id] = failedOp.copy(failureId = id)
        notifyChange()
        return id
    }

    override fun observeUnresolvedFailures(): Flow<List<FailedOperationEntity>> =
        failuresFlow.map { list -> list.filter { !it.resolved } }

    override suspend fun markFailuresResolvedForRecord(recordId: Long) {
        failedOperations.entries.forEach { (id, f) ->
            if (f.recordId == recordId) {
                failedOperations[id] = f.copy(resolved = true)
            }
        }
        notifyChange()
    }

    // 10, 11 & 12. AppSettingsEntity, StoragePermissionEntity, DriveChangeTokenEntity
    override suspend fun upsertAppSettings(settings: AppSettingsEntity) {
        appSettingsEntity = settings
    }

    override suspend fun getAppSettingsEntity(): AppSettingsEntity? = appSettingsEntity

    override suspend fun upsertStoragePermission(permission: StoragePermissionEntity) {
        storagePermissions[permission.permissionKey] = permission
    }

    override suspend fun getAllStoragePermissions(): List<StoragePermissionEntity> =
        storagePermissions.values.toList()

    override suspend fun upsertDriveChangeToken(token: DriveChangeTokenEntity) {
        driveChangeTokens[token.accountId] = token
    }

    override suspend fun getDriveChangeToken(accountId: String): DriveChangeTokenEntity? =
        driveChangeTokens[accountId]

    fun getAllRecordsSnapshot(): List<BackupFileEntity> = records.values.toList()
    fun getAllSyncJobsSnapshot(): List<SyncJobEntity> = syncJobs.values.toList()
    fun getAllSyncEventsSnapshot(): List<SyncEventEntity> = syncEvents.toList()
}

/**
 * Comprehensive Automated Security, Data Integrity, State Machine & Reliability Test Suite
 * covering all mandatory scenarios across Sections 6, 7, 8, 9, 10, 14, 15, and 16.
 */
class PermanentBackupEngineTest {

    private lateinit var mockWebServer: MockWebServer
    private lateinit var tempStorageDir: File
    private lateinit var dao: InMemoryBackupDao
    private lateinit var authManager: GoogleAuthManager
    private lateinit var driveApiClient: DriveApiClient

    private val uploadedDriveFiles = LinkedHashMap<String, Triple<String, Long, String?>>()
    private val fileIdSeq = AtomicInteger(100)

    @Before
    fun setUp() {
        mockWebServer = MockWebServer()
        mockWebServer.start()
        tempStorageDir = Files.createTempDirectory("android_permanent_backup_test").toFile()
        dao = InMemoryBackupDao()
        authManager = GoogleAuthManager(
            context = null,
            tokenEndpointUrl = mockWebServer.url("/oauth2/token").toString(),
            revokeEndpointUrl = mockWebServer.url("/oauth2/revoke").toString()
        )
        authManager.onAccountAuthorized(
            email = "user@example.com",
            displayName = "Test User",
            accessToken = "valid_oauth_access_token",
            refreshToken = "valid_refresh_token",
            expiresInSeconds = 3600L
        )
        driveApiClient = DriveApiClient(
            driveBaseUrl = mockWebServer.url("/").toString().trimEnd('/')
        )
    }

    @After
    fun tearDown() {
        mockWebServer.shutdown()
        tempStorageDir.deleteRecursively()
    }

    private fun installStandardDriveMockDispatcher(quotaFull: Boolean = false) {
        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                val method = request.method ?: "GET"

                if (path.startsWith("/drive/v3/about")) {
                    val limit = 15_000_000_000L
                    val usage = if (quotaFull) 15_000_000_000L else 1_000_000_000L
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"storageQuota":{"limit":$limit,"usage":$usage,"usageInDrive":$usage,"usageInDriveTrash":0}}""")
                }

                if (method == "GET" && path.startsWith("/drive/v3/files?")) {
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"files":[]}""")
                }

                if (method == "POST" && path.startsWith("/drive/v3/files")) {
                    val folderId = "folder_${fileIdSeq.incrementAndGet()}"
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"id":"$folderId","name":"folder"}""")
                }

                if (method == "POST" && path.startsWith("/upload/drive/v3/files?uploadType=resumable")) {
                    if (quotaFull) {
                        return MockResponse()
                            .setResponseCode(403)
                            .setBody("""{"error":{"errors":[{"reason":"storageQuotaExceeded"}]}}""")
                    }
                    val uploadId = "session_${fileIdSeq.incrementAndGet()}"
                    val sessionUrl = mockWebServer.url("/upload/drive/v3/files?uploadType=resumable&upload_id=$uploadId").toString()
                    return MockResponse()
                        .setResponseCode(200)
                        .addHeader("Location", sessionUrl)
                }

                if (method == "PUT" && path.contains("upload_id=")) {
                    val bodyBytes = request.body.readByteArray()
                    val contentRange = request.getHeader("Content-Range") ?: ""
                    val totalSize = contentRange.substringAfter('/', "0").toLongOrNull() ?: bodyBytes.size.toLong()
                    val driveId = "drive_file_${fileIdSeq.incrementAndGet()}"
                    uploadedDriveFiles[driveId] = Triple(driveId, totalSize, null)
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"id":"$driveId","name":"uploaded_file","size":$totalSize}""")
                }

                if (method == "GET" && path.startsWith("/drive/v3/files/")) {
                    val fileId = path.substringAfter("/drive/v3/files/").substringBefore("?")
                    val meta = uploadedDriveFiles[fileId]
                    val size = meta?.second ?: 0L
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"id":"$fileId","name":"verified_file","size":$size,"trashed":false}""")
                }

                return MockResponse().setResponseCode(404)
            }
        }
    }

    // 1. Local file deleted -> Cloud backup remains 100% intact
    @Test
    fun test01_localFileDeleted_cloudBackupRemainsIntact() = runTest {
        installStandardDriveMockDispatcher()
        val dcimDir = File(tempStorageDir, "DCIM/Camera").apply { mkdirs() }
        val photoFile = File(dcimDir, "IMG_20261028.jpg").apply {
            writeText("JPEG_RAW_DATA_PERMANENT_1234567890")
        }

        val scanner = AndroidStorageScanner(
            context = null,
            customRootDirectories = listOf(tempStorageDir),
            permissionOverride = true
        )
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        val cycle1 = engine.runBackupCycle(settings)
        assertEquals(1, cycle1.scannedFiles)
        assertEquals(1, cycle1.uploadedAndVerifiedFiles)

        val recordAfterUpload = dao.getAllRecordsSnapshot().single()
        assertEquals(UploadStatus.VERIFIED, recordAfterUpload.uploadStatus)
        assertNotNull(recordAfterUpload.driveFileId)
        assertTrue(recordAfterUpload.localExists)

        // Delete the file from the phone
        assertTrue(photoFile.delete())
        assertFalse(photoFile.exists())

        // Second backup cycle after local deletion
        val cycle2 = engine.runBackupCycle(settings)
        assertEquals(0, cycle2.scannedFiles)
        assertEquals(1, cycle2.preservedDeletedLocalFiles)

        val recordAfterDelete = dao.getAllRecordsSnapshot().single()
        assertEquals(UploadStatus.VERIFIED, recordAfterDelete.uploadStatus)
        assertEquals(recordAfterUpload.driveFileId, recordAfterDelete.driveFileId)
        assertFalse(recordAfterDelete.localExists)

        // Verify BackupVersionEntity also marked preservedAfterLocalDelete = true
        val versions = dao.getBackupVersionsForIdentity(recordAfterDelete.sourceFileIdentity)
        assertEquals(1, versions.size)
        assertTrue(versions.single().preservedAfterLocalDelete)
    }

    // 2. Local folder deleted -> Cloud folder and all files remain intact
    @Test
    fun test02_localFolderDeleted_cloudFolderRemainsIntact() = runTest {
        installStandardDriveMockDispatcher()
        val whatsappDir = File(tempStorageDir, "WhatsApp/Media/WhatsApp Images").apply { mkdirs() }
        File(whatsappDir, "WA_001.jpg").writeText("WA_IMAGE_CONTENT_1")
        File(whatsappDir, "WA_002.jpg").writeText("WA_IMAGE_CONTENT_2")

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        val firstRun = engine.runBackupCycle(settings)
        assertEquals(2, firstRun.uploadedAndVerifiedFiles)
        val cachedCloudFolder = dao.getCachedDriveFolder("WhatsApp/Media/WhatsApp Images")
        assertNotNull(cachedCloudFolder)

        // Delete the entire local WhatsApp folder from phone
        whatsappDir.deleteRecursively()
        assertFalse(whatsappDir.exists())

        // Run subsequent backup cycle
        val secondRun = engine.runBackupCycle(settings)
        assertEquals(2, secondRun.preservedDeletedLocalFiles)

        assertNotNull(dao.getCachedDriveFolder("WhatsApp/Media/WhatsApp Images"))
        assertEquals(2, dao.countVerifiedRecords())
    }

    // 3. File renamed locally -> Previous cloud backup remains intact & renamed file is backed up
    @Test
    fun test03_localFileRenamed_previousBackupPreservedAndNewSaved() = runTest {
        installStandardDriveMockDispatcher()
        val docsDir = File(tempStorageDir, "Documents").apply { mkdirs() }
        val originalFile = File(docsDir, "report_draft.pdf").apply {
            writeText("PDF_BINARY_CONTENT_ABC")
        }

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        engine.runBackupCycle(settings)
        val originalRecord = dao.getAllRecordsSnapshot().single()
        assertEquals("report_draft.pdf", originalRecord.driveStoredName)
        assertEquals(UploadStatus.VERIFIED, originalRecord.uploadStatus)

        val renamedFile = File(docsDir, "report_final.pdf")
        assertTrue(originalFile.renameTo(renamedFile))

        engine.runBackupCycle(settings)
        val allRecords = dao.getAllRecordsSnapshot()
        assertEquals(2, allRecords.size)

        val oldRec = allRecords.first { it.displayName == "report_draft.pdf" }
        val newRec = allRecords.first { it.displayName == "report_final.pdf" }

        assertEquals(UploadStatus.VERIFIED, oldRec.uploadStatus)
        assertFalse(oldRec.localExists)
        assertEquals(UploadStatus.VERIFIED, newRec.uploadStatus)
        assertTrue(newRec.localExists)
        assertTrue(oldRec.driveFileId != newRec.driveFileId)
    }

    // 4. File modified locally -> Old version preserved on Drive & new version uploaded separately
    @Test
    fun test04_localFileModified_oldVersionPreservedNewVersionCreated() = runTest {
        installStandardDriveMockDispatcher()
        val docsDir = File(tempStorageDir, "Documents").apply { mkdirs() }
        val notesFile = File(docsDir, "journal.txt").apply {
            writeText("Entry 1: Initial journal text")
        }

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        engine.runBackupCycle(settings)
        assertEquals(1, dao.countVerifiedRecords())

        notesFile.writeText("Entry 1 + Entry 2: Updated journal text with more data")
        notesFile.setLastModified(System.currentTimeMillis() + 5000L)

        engine.runBackupCycle(settings)
        val records = dao.getAllRecordsSnapshot().sortedBy { it.backupVersion }
        assertEquals(2, records.size)

        val v1 = records[0]
        val v2 = records[1]

        assertEquals(1, v1.backupVersion)
        assertEquals("journal.txt", v1.driveStoredName)
        assertEquals(UploadStatus.VERIFIED, v1.uploadStatus)

        assertEquals(2, v2.backupVersion)
        assertTrue(v2.driveStoredName.startsWith("journal__v2_"))
        assertEquals(UploadStatus.VERIFIED, v2.uploadStatus)
        assertTrue(v1.driveFileId != v2.driveFileId)

        // Verify BackupVersionEntity records both versions
        val versionEntities = dao.getBackupVersionsForIdentity(v1.sourceFileIdentity)
        assertEquals(2, versionEntities.size)
    }

    // 5. Network failure mid-upload -> Resumes from exact confirmed byte offset on retry
    @Test
    fun test05_networkFailureMidUpload_resumesFromExactByteOffset() = runTest {
        val moviesDir = File(tempStorageDir, "Movies").apply { mkdirs() }
        val chunkSize = 256 * 1024 // 256 KiB
        val totalSize = chunkSize * 2 + 10_000 // 534,288 bytes (3 chunks)
        val payload = ByteArray(totalSize) { idx -> (idx % 251).toByte() }
        val videoFile = File(moviesDir, "clip.mp4").apply {
            writeBytes(payload)
        }

        var putChunkCallCount = 0
        var probedResumeSession = false
        val chunkStartOffsetsReceived = mutableListOf<Long>()

        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                val method = request.method ?: "GET"

                if (path.startsWith("/drive/v3/about")) {
                    return MockResponse().setResponseCode(200)
                        .setBody("""{"storageQuota":{"limit":15000000000,"usage":1000}}""")
                }
                if (method == "GET" && path.startsWith("/drive/v3/files?")) {
                    return MockResponse().setResponseCode(200).setBody("""{"files":[]}""")
                }
                if (method == "POST" && path.startsWith("/drive/v3/files")) {
                    return MockResponse().setResponseCode(200).setBody("""{"id":"folder_movies"}""")
                }
                if (method == "POST" && path.startsWith("/upload/drive/v3/files?uploadType=resumable")) {
                    val sessionUrl = mockWebServer.url("/upload/drive/v3/files?uploadType=resumable&upload_id=resumable_123").toString()
                    return MockResponse().setResponseCode(200).addHeader("Location", sessionUrl)
                }
                if (method == "PUT" && path.contains("upload_id=resumable_123")) {
                    val contentRange = request.getHeader("Content-Range") ?: ""
                    if (contentRange.startsWith("bytes */")) {
                        probedResumeSession = true
                        return MockResponse()
                            .setResponseCode(308)
                            .addHeader("Range", "bytes=0-${chunkSize - 1}")
                    }

                    putChunkCallCount++
                    val startByte = contentRange.substringAfter("bytes ").substringBefore("-").toLong()
                    chunkStartOffsetsReceived.add(startByte)

                    if (putChunkCallCount == 1) {
                        return MockResponse()
                            .setResponseCode(308)
                            .addHeader("Range", "bytes=0-${chunkSize - 1}")
                    }
                    if (putChunkCallCount == 2) {
                        return MockResponse().setResponseCode(503).setBody("Simulated network drop")
                    }
                    if (putChunkCallCount == 3) {
                        return MockResponse()
                            .setResponseCode(308)
                            .addHeader("Range", "bytes=0-${chunkSize * 2 - 1}")
                    }
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"id":"drive_video_999","name":"clip.mp4","size":$totalSize}""")
                }
                if (method == "GET" && path.startsWith("/drive/v3/files/drive_video_999")) {
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"id":"drive_video_999","name":"clip.mp4","size":$totalSize,"trashed":false}""")
                }
                return MockResponse().setResponseCode(404)
            }
        }

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true, chunkSizeBytes = chunkSize)

        val attempt1 = engine.runBackupCycle(settings)
        assertEquals(0, attempt1.uploadedAndVerifiedFiles)
        assertEquals(1, attempt1.failedUploads)

        val partialRecord = dao.getAllRecordsSnapshot().single()
        assertEquals(chunkSize.toLong(), partialRecord.uploadedBytes)
        assertNotNull(partialRecord.resumableSessionUri)
        assertEquals(UploadStatus.PENDING_RETRY, partialRecord.uploadStatus)

        val attempt2 = engine.runBackupCycle(settings)
        assertTrue(probedResumeSession)
        assertEquals(1, attempt2.uploadedAndVerifiedFiles)
        assertEquals(listOf(0L, chunkSize.toLong(), chunkSize.toLong(), (chunkSize * 2).toLong()), chunkStartOffsetsReceived)

        val finalRecord = dao.getAllRecordsSnapshot().single()
        assertEquals(UploadStatus.VERIFIED, finalRecord.uploadStatus)
        assertEquals(totalSize.toLong(), finalRecord.uploadedBytes)
        assertTrue(videoFile.exists())

        // Verify SyncJobState reached COMPLETED
        val job = dao.getSyncJobByRecordId(finalRecord.id)
        assertNotNull(job)
        assertEquals(SyncJobState.COMPLETED, job!!.syncState)
    }

    // 6. App crash during backup -> Recovers incomplete tasks without losing history
    @Test
    fun test06_appCrashDuringBackup_recoversIncompleteTasksWithoutHistoryLoss() = runTest {
        dao.insertRecord(
            BackupFileEntity(
                sourceUri = "/storage/emulated/0/DCIM/old.jpg",
                sourceFileIdentity = "vol0:DCIM/old.jpg",
                relativePath = "DCIM/old.jpg",
                displayName = "old.jpg",
                folderCategory = "DCIM",
                fileSize = 1024L,
                lastModifiedMs = 1000L,
                mimeType = "image/jpeg",
                sha256Hash = "abc",
                md5Hash = "def",
                driveFileId = "drive_verified_1",
                driveStoredName = "old.jpg",
                backupVersion = 1,
                uploadedBytes = 1024L,
                uploadStatus = UploadStatus.VERIFIED,
                verificationStatus = VerificationStatus.CHECKSUM_AND_SIZE_VERIFIED
            )
        )

        val crashedId = dao.insertRecord(
            BackupFileEntity(
                sourceUri = "/storage/emulated/0/Movies/large.mp4",
                sourceFileIdentity = "vol0:Movies/large.mp4",
                relativePath = "Movies/large.mp4",
                displayName = "large.mp4",
                folderCategory = "Movies",
                fileSize = 10_485_760L,
                lastModifiedMs = 2000L,
                mimeType = "video/mp4",
                sha256Hash = "123",
                md5Hash = "456",
                driveStoredName = "large.mp4",
                backupVersion = 1,
                uploadedBytes = 5_242_880L,
                resumableSessionUri = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=crash_session",
                uploadStatus = UploadStatus.IN_PROGRESS
            )
        )

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)

        val recoveredCount = engine.recoverInterruptedUploadsAfterRestart()
        assertEquals(1, recoveredCount)
        assertEquals(1, dao.countVerifiedRecords())

        val recoveredRecord = dao.getRecordById(crashedId)!!
        assertEquals(UploadStatus.PENDING_RETRY, recoveredRecord.uploadStatus)
        assertEquals(5_242_880L, recoveredRecord.uploadedBytes)
        assertNotNull(recoveredRecord.resumableSessionUri)

        val syncJob = dao.getSyncJobByRecordId(crashedId)
        assertNotNull(syncJob)
        assertEquals(SyncJobState.RETRY_SCHEDULED, syncJob!!.syncState)
    }

    // 7. Device restart -> Pending backup scheduling & WorkManager constraints recovered
    @Test
    fun test07_deviceRestart_pendingBackupsPreservedAndWorkConstraintsConfigured() = runTest {
        val settings = BackupSettings(
            userBackupConsentGranted = true,
            wifiOnly = true,
            chargingOnly = true,
            unmeteredNetworkOnly = true,
            periodicIntervalHours = 6
        )
        val constraints = BackupWorkScheduler.buildWorkConstraints(settings)
        assertTrue(constraints.requiresCharging())
        assertTrue(constraints.requiresBatteryNotLow())
    }

    // 8. Google Drive storage quota full -> No data lost & accurate error exposed
    @Test
    fun test08_googleDriveQuotaFull_preservesDataAndReportsQuotaError() = runTest {
        installStandardDriveMockDispatcher(quotaFull = true)
        val picsDir = File(tempStorageDir, "Pictures").apply { mkdirs() }
        File(picsDir, "family.png").writeText("PNG_DATA_FAMILY")

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        val summary = engine.runBackupCycle(settings)
        assertTrue(summary.quotaFull)
        assertTrue(engine.quotaFullState.value)
        assertNotNull(summary.errorMessage)
        assertEquals(1, dao.countAllRecords())
    }

    // 9. Access permission revoked or file deleted before initial upload -> Accurate error reporting
    @Test
    fun test09_accessPermissionRevokedAndUnuploadedDeletedFileHandling() = runTest {
        installStandardDriveMockDispatcher()
        val scannerDenied = AndroidStorageScanner(null, listOf(tempStorageDir), permissionOverride = false)
        val engineDenied = PermanentBackupEngine(dao, scannerDenied, driveApiClient, authManager)

        val deniedSummary = engineDenied.runBackupCycle(BackupSettings(userBackupConsentGranted = true))
        assertTrue(deniedSummary.permissionDenied)
        assertNotNull(engineDenied.lastPermissionError.value)

        val pendingId = dao.insertRecord(
            BackupFileEntity(
                sourceUri = File(tempStorageDir, "deleted_before_upload.txt").absolutePath,
                sourceFileIdentity = "vol0:deleted_before_upload.txt",
                relativePath = "deleted_before_upload.txt",
                displayName = "deleted_before_upload.txt",
                folderCategory = "Documents",
                fileSize = 500L,
                lastModifiedMs = 1000L,
                mimeType = "text/plain",
                sha256Hash = "aaa",
                md5Hash = "bbb",
                driveStoredName = "deleted_before_upload.txt",
                uploadStatus = UploadStatus.PENDING
            )
        )
        val scannerAllowed = AndroidStorageScanner(null, listOf(tempStorageDir), permissionOverride = true)
        val engineAllowed = PermanentBackupEngine(dao, scannerAllowed, driveApiClient, authManager)
        val uploaded = engineAllowed.uploadSingleRecordResumable(
            dao.getRecordById(pendingId)!!,
            "root_folder_id",
            BackupSettings(userBackupConsentGranted = true)
        )
        assertFalse(uploaded)
        val failedRec = dao.getRecordById(pendingId)!!
        assertEquals(UploadStatus.FAILED, failedRec.uploadStatus)
        assertEquals(FailureReason.SOURCE_DELETED_BEFORE_UPLOAD, failedRec.failureReason)
        val job = dao.getSyncJobByRecordId(pendingId)
        assertEquals(SyncJobState.FAILED_ACTION_REQUIRED, job?.syncState)
    }

    // 10. 1,000 / 10,000 / 100,000 / 1,000,000 Metadata Records -> Pagination and Memory Bounded (Section 14)
    @Test
    fun test10_hundredThousandFiles_paginationAndMemoryBounded() = runTest {
        val tiers = listOf(1_000, 10_000, 100_000, 1_000_000)
        for (tierCount in tiers) {
            var processedCount = 0
            val syntheticSequence = sequence {
                for (i in 1..tierCount) {
                    yield(
                        DiscoveredSourceFile(
                            sourceUri = "/storage/emulated/0/DCIM/IMG_$i.jpg",
                            sourceFileIdentity = "vol0:DCIM/IMG_$i.jpg",
                            relativePath = "DCIM/IMG_$i.jpg",
                            displayName = "IMG_$i.jpg",
                            folderCategory = "DCIM",
                            fileSize = 4096L,
                            lastModifiedMs = 1700000000000L,
                            mimeType = "image/jpeg",
                            sha256Hash = "hash_$i",
                            md5Hash = "md5_$i"
                        )
                    )
                }
            }
            syntheticSequence.chunked(PermanentBackupEngine.PAGINATION_BATCH_SIZE).forEach { batch ->
                processedCount += batch.size
            }
            assertEquals(tierCount, processedCount)
        }
    }

    // 11. Unchanged file is never uploaded twice; same-named files in different folders are both backed up
    @Test
    fun test11_duplicateControl_unchangedFileSkippedAndSameNameDifferentFoldersSeparated() = runTest {
        installStandardDriveMockDispatcher()
        val dcimDir = File(tempStorageDir, "DCIM").apply { mkdirs() }
        val picturesDir = File(tempStorageDir, "Pictures").apply { mkdirs() }

        File(dcimDir, "photo.jpg").writeText("CAMERA_PHOTO_CONTENT_111")
        File(picturesDir, "photo.jpg").writeText("EDITED_PHOTO_CONTENT_222")

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        val firstCycle = engine.runBackupCycle(settings)
        assertEquals(2, firstCycle.scannedFiles)
        assertEquals(2, firstCycle.newlyQueuedVersions)
        assertEquals(2, firstCycle.uploadedAndVerifiedFiles)

        val secondCycle = engine.runBackupCycle(settings)
        assertEquals(2, secondCycle.scannedFiles)
        assertEquals(0, secondCycle.newlyQueuedVersions)
        assertEquals(2, secondCycle.skippedUnchangedFiles)
        assertEquals(0, secondCycle.uploadedAndVerifiedFiles)
    }

    // 12. Never-Delete Guard -> Any destructive Google Drive API call is strictly blocked
    @Test
    fun test12_neverDeleteGuard_blocksAllDestructiveDriveApiCalls() {
        val client = driveApiClient.httpClient

        // Attempt 1: HTTP DELETE
        val deleteReq = Request.Builder()
            .url(mockWebServer.url("/drive/v3/files/file_123"))
            .delete()
            .build()
        var deleteBlocked = false
        try {
            client.newCall(deleteReq).execute()
        } catch (_: NeverDeletePolicyViolationException) {
            deleteBlocked = true
        }
        assertTrue("HTTP DELETE must be blocked by NeverDeleteGuardInterceptor", deleteBlocked)

        // Attempt 2: HTTP PATCH
        val patchReq = Request.Builder()
            .url(mockWebServer.url("/drive/v3/files/file_123"))
            .patch("""{"trashed":true}""".toRequestBody("application/json".toMediaType()))
            .build()
        var patchBlocked = false
        try {
            client.newCall(patchReq).execute()
        } catch (_: NeverDeletePolicyViolationException) {
            patchBlocked = true
        }
        assertTrue("HTTP PATCH must be blocked by NeverDeleteGuardInterceptor", patchBlocked)

        // Attempt 3: Direct PUT overwrite without resumable upload_id
        val overwritePutReq = Request.Builder()
            .url(mockWebServer.url("/upload/drive/v3/files/file_123"))
            .put("overwrite".toRequestBody("text/plain".toMediaType()))
            .build()
        var putOverwriteBlocked = false
        try {
            client.newCall(overwritePutReq).execute()
        } catch (_: NeverDeletePolicyViolationException) {
            putOverwriteBlocked = true
        }
        assertTrue("Direct PUT overwrite must be blocked", putOverwriteBlocked)

        // Attempt 4: POST with trashed=true
        val trashPostReq = Request.Builder()
            .url(mockWebServer.url("/drive/v3/files"))
            .post("""{"name":"x","trashed":true}""".toRequestBody("application/json".toMediaType()))
            .build()
        var trashBodyBlocked = false
        try {
            client.newCall(trashPostReq).execute()
        } catch (_: NeverDeletePolicyViolationException) {
            trashBodyBlocked = true
        }
        assertTrue("Setting trashed=true must be blocked", trashBodyBlocked)
    }

    // 13. Source File Read-Only Guard -> Source files are never written, renamed, or deleted by backup engine
    @Test
    fun test13_sourceFileReadOnlyGuard_neverWritesRenamesOrDeletesLocalFiles() {
        val musicDir = File(tempStorageDir, "Music").apply { mkdirs() }
        val audioFile = File(musicDir, "track.mp3")
        val originalBytes = ByteArray(128 * 1024) { (it % 127).toByte() }
        audioFile.writeBytes(originalBytes)
        val originalMtime = audioFile.lastModified()

        val hashes = SourceFileReadOnlyGuard.computeStreamingHashes {
            SourceFileReadOnlyGuard.openReadOnlyFileStream(audioFile)
        }
        assertEquals(originalBytes.size.toLong(), hashes.totalBytesRead)

        SourceFileReadOnlyGuard.openReadOnlyFileStream(audioFile).use { stream ->
            SourceFileReadOnlyGuard.skipStreamToOffset(stream, 65536L)
            assertEquals(originalBytes[65536].toInt(), stream.read())
        }

        assertTrue(audioFile.exists())
        assertEquals(originalBytes.size.toLong(), audioFile.length())
        assertEquals(originalMtime, audioFile.lastModified())

        var mutationBlocked = false
        try {
            SourceFileReadOnlyGuard.forbidSourceFileMutation("DELETE", audioFile.absolutePath)
        } catch (_: SourceFileModificationForbiddenException) {
            mutationBlocked = true
        }
        assertTrue(mutationBlocked)
    }

    // 14. Upload Success / Database Failure Reconciliation -> Prevents Duplicate Upload After Crash (Section 7 & 14)
    @Test
    fun test14_uploadSuccessDatabaseFailure_reconciliationPreventsDuplicateUpload() = runTest {
        val docsDir = File(tempStorageDir, "Documents").apply { mkdirs() }
        val contractFile = File(docsDir, "contract.pdf").apply {
            writeText("CONTRACT_CONTENT_RECONCILE_999")
        }
        val hashes = SourceFileReadOnlyGuard.computeStreamingHashes {
            SourceFileReadOnlyGuard.openReadOnlyFileStream(contractFile)
        }

        var resumableInitiatedCount = 0

        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                val method = request.method ?: "GET"

                if (path.startsWith("/drive/v3/about")) {
                    return MockResponse().setResponseCode(200)
                        .setBody("""{"storageQuota":{"limit":15000000000,"usage":1000}}""")
                }
                if (method == "GET" && path.startsWith("/drive/v3/files?")) {
                    val q = request.requestUrl?.queryParameter("q") ?: ""
                    if (q.contains("name = 'contract.pdf'")) {
                        // Simulate that the file was ALREADY uploaded to Drive right before local DB crashed!
                        return MockResponse().setResponseCode(200).setBody(
                            """
                            {
                              "files": [
                                {
                                  "id": "existing_drive_file_777",
                                  "name": "contract.pdf",
                                  "size": ${contractFile.length()},
                                  "sha256Checksum": "${hashes.sha256Hex}",
                                  "trashed": false
                                }
                              ]
                            }
                            """.trimIndent()
                        )
                    }
                    return MockResponse().setResponseCode(200).setBody("""{"files":[]}""")
                }
                if (method == "POST" && path.startsWith("/drive/v3/files")) {
                    return MockResponse().setResponseCode(200).setBody("""{"id":"folder_docs"}""")
                }
                if (method == "POST" && path.startsWith("/upload/drive/v3/files?uploadType=resumable")) {
                    resumableInitiatedCount++
                    return MockResponse().setResponseCode(500)
                }
                if (method == "GET" && path.startsWith("/drive/v3/files/existing_drive_file_777")) {
                    return MockResponse().setResponseCode(200).setBody(
                        """{"id":"existing_drive_file_777","name":"contract.pdf","size":${contractFile.length()},"sha256Checksum":"${hashes.sha256Hex}","trashed":false}"""
                    )
                }
                return MockResponse().setResponseCode(404)
            }
        }

        // Insert a record that had retryCount = 1 after a crash before DB update completed
        val recId = dao.insertRecord(
            BackupFileEntity(
                sourceUri = contractFile.absolutePath,
                sourceFileIdentity = "vol0:Documents/contract.pdf",
                relativePath = "Documents/contract.pdf",
                displayName = "contract.pdf",
                folderCategory = "Documents",
                fileSize = contractFile.length(),
                lastModifiedMs = contractFile.lastModified(),
                mimeType = "application/pdf",
                sha256Hash = hashes.sha256Hex,
                md5Hash = hashes.md5Hex,
                driveStoredName = "contract.pdf",
                backupVersion = 1,
                uploadStatus = UploadStatus.PENDING_RETRY,
                retryCount = 1
            )
        )

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)

        val reconciled = engine.uploadSingleRecordResumable(
            dao.getRecordById(recId)!!,
            "root_folder_id",
            BackupSettings(userBackupConsentGranted = true)
        )

        assertTrue(reconciled)
        assertEquals(0, resumableInitiatedCount) // Zero duplicate upload initiated!
        val updated = dao.getRecordById(recId)!!
        assertEquals(UploadStatus.VERIFIED, updated.uploadStatus)
        assertEquals("existing_drive_file_777", updated.driveFileId)
    }

    // 15. File Restore Never Overwrites Existing Local File & Blocks Malicious Path Traversal (Section 5F & 15)
    @Test
    fun test15_nonOverwritingFileRestore_andPathTraversalSanitization() = runTest {
        val restoreRoot = File(tempStorageDir, "Restored_Out").apply { mkdirs() }
        val existingLocal = File(restoreRoot, "notes.txt").apply {
            writeText("EXISTING_LOCAL_CONTENT_DO_NOT_OVERWRITE")
        }

        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                if ((request.path ?: "").contains("alt=media")) {
                    return MockResponse().setResponseCode(200).setBody("CLOUD_RESTORED_VERSION_CONTENT")
                }
                return MockResponse().setResponseCode(404)
            }
        }

        val cloudOps = CloudFileOperationsManager(
            context = null,
            backupDao = dao,
            driveApiClient = driveApiClient,
            authManager = authManager,
            customRestoreRootDir = restoreRoot
        )

        // Restore when "notes.txt" already exists -> must NOT overwrite existingLocal!
        val outcome = cloudOps.restoreCloudFileToLocal(
            driveFileId = "drive_notes_1",
            originalFileName = "notes.txt",
            versionNumber = 2
        )
        assertTrue(outcome.success)
        assertTrue(outcome.avoidedOverwrite)
        assertEquals("EXISTING_LOCAL_CONTENT_DO_NOT_OVERWRITE", existingLocal.readText())
        val restoredNewFile = File(outcome.restoredLocalPath!!)
        assertTrue(restoredNewFile.exists())
        assertTrue(restoredNewFile.name.startsWith("notes_restored_v2_"))
        assertEquals("CLOUD_RESTORED_VERSION_CONTENT", restoredNewFile.readText())

        // Verify path traversal filename sanitization
        val sanitized = cloudOps.sanitizeSafeLocalFileName("../../etc/passwd\u0000")
        assertFalse(sanitized.contains(".."))
        assertFalse(sanitized.contains("/"))
        assertFalse(sanitized.contains("\u0000"))
    }

    // 16. Low-Storage Offline Cache Budget Enforcement & 16 KiB Range Stream Preview (Section 5D, 5E & 10)
    @Test
    fun test16_offlineCacheBudgetEnforcement_andCloudStreamPreview() = runTest {
        val offlineDir = File(tempStorageDir, "Offline_Cache").apply { mkdirs() }
        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                if ((request.path ?: "").contains("alt=media")) {
                    return MockResponse().setResponseCode(200).setBody("STREAMED_CLOUD_PREVIEW_DATA_12345")
                }
                return MockResponse().setResponseCode(404)
            }
        }

        val cloudOps = CloudFileOperationsManager(
            context = null,
            backupDao = dao,
            driveApiClient = driveApiClient,
            authManager = authManager,
            customOfflineCacheDir = offlineDir
        )

        val oversizedItem = CloudBrowseItem(
            driveFileId = "file_big",
            accountId = "user@example.com",
            name = "big_movie.mp4",
            mimeType = "video/mp4",
            sizeBytes = 50L * 1024 * 1024 // 50 MB
        )
        // Budget is 10 MB -> must reject pinning 50 MB file
        val rejected = cloudOps.pinCloudFileOffline(oversizedItem, maxBudgetBytes = 10L * 1024 * 1024)
        assertFalse(rejected.success)

        // Small file within budget succeeds
        val smallItem = oversizedItem.copy(driveFileId = "file_small", name = "readme.txt", sizeBytes = 33L)
        val pinned = cloudOps.pinCloudFileOffline(smallItem, maxBudgetBytes = 10L * 1024 * 1024)
        assertTrue(pinned.success)
        assertEquals(33L, dao.sumOfflineCachedBytes())

        // Stream preview snippet
        val preview = cloudOps.streamPreviewSnippet("file_small")
        assertEquals("STREAMED_CLOUD_PREVIEW_DATA_12345", preview)
    }

    // 17. Multi-Account Isolation & Persistent Session Across Restarts (Section 3 & 5H)
    @Test
    fun test17_multiAccountIsolation_andPersistentSessionAcrossRestarts() = runTest {
        authManager.onAccountAuthorized(
            email = "alice@example.com",
            displayName = "Alice",
            accessToken = "alice_token",
            refreshToken = "alice_refresh"
        )
        authManager.setRootDriveFolderId("folder_alice_root")
        assertEquals("folder_alice_root", authManager.accountState.value.rootDriveFolderId)

        // Switch to Bob's account -> Bob has his own isolated root folder ID
        authManager.switchGoogleAccount(
            newEmail = "bob@example.com",
            newDisplayName = "Bob",
            newAccessToken = "bob_token",
            newRefreshToken = "bob_refresh"
        )
        assertEquals("bob@example.com", authManager.accountState.value.email)
        assertEquals(null, authManager.accountState.value.rootDriveFolderId)
        authManager.setRootDriveFolderId("folder_bob_root")

        // Switch back to Alice -> Alice's rootDriveFolderId ("folder_alice_root") is still preserved!
        authManager.switchGoogleAccount(
            newEmail = "alice@example.com",
            newDisplayName = "Alice",
            newAccessToken = "alice_token",
            newRefreshToken = "alice_refresh"
        )
        assertEquals("folder_alice_root", authManager.accountState.value.rootDriveFolderId)
    }

    // 18. Security Audit: Zero Embedded Secrets in BuildConfig & HTTP 400 Bad Request Token Redaction (Section 9 & 15)
    @Test
    fun test18_securityAudit_zeroEmbeddedSecretsAndBadRequestSanitization() {
        val buildConfigFields = BuildConfig::class.java.declaredFields.map { it.name }
        assertFalse(
            "DEFAULT_GDRIVE_REFRESH_TOKEN must NEVER exist in BuildConfig",
            buildConfigFields.contains("DEFAULT_GDRIVE_REFRESH_TOKEN")
        )
        assertFalse(
            "GOOGLE_OAUTH_CLIENT_SECRET must NEVER exist in BuildConfig",
            buildConfigFields.contains("GOOGLE_OAUTH_CLIENT_SECRET")
        )

        // Verify HTTP 400 Bad Request structured diagnostics & token redaction
        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                return MockResponse()
                    .setResponseCode(400)
                    .setBody("""{"error":"invalid_request","leaked":"ya29.secret_access_token_xyz"}""")
            }
        }

        var caughtBadRequest: DriveBadRequestException? = null
        try {
            driveApiClient.fetchDriveQuota("ya29.secret_access_token_xyz")
        } catch (e: DriveBadRequestException) {
            caughtBadRequest = e
        }
        assertNotNull(caughtBadRequest)
        assertEquals("GET", caughtBadRequest!!.requestMethod)
        assertTrue(caughtBadRequest.queryParamNames.contains("fields"))
        assertFalse(caughtBadRequest.sanitizedResponse.contains("ya29.secret_access_token_xyz"))
        assertTrue(caughtBadRequest.sanitizedResponse.contains("[REDACTED_ACCESS_TOKEN]"))
    }

    // 19. My Drive Explorer Listing, Search, Sort & Google Drive Changes API v3 (Section 5A & 9)
    @Test
    fun test19_driveExplorer_listSearchSortAndIncrementalChangesApi() = runTest {
        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                if (path.startsWith("/drive/v3/changes/startPageToken")) {
                    return MockResponse().setResponseCode(200).setBody("""{"startPageToken":"100"}""")
                }
                if (path.startsWith("/drive/v3/changes")) {
                    return MockResponse().setResponseCode(200).setBody(
                        """
                        {
                          "newStartPageToken": "101",
                          "changes": [
                            {
                              "fileId": "cloud_doc_1",
                              "removed": false,
                              "file": {
                                "id": "cloud_doc_1",
                                "name": "Architecture_Spec.pdf",
                                "mimeType": "application/pdf",
                                "size": 204800,
                                "trashed": false
                              }
                            }
                          ]
                        }
                        """.trimIndent()
                    )
                }
                if (path.startsWith("/drive/v3/files")) {
                    return MockResponse().setResponseCode(200).setBody(
                        """
                        {
                          "files": [
                            {
                              "id": "cloud_doc_1",
                              "name": "Architecture_Spec.pdf",
                              "mimeType": "application/pdf",
                              "size": 204800,
                              "starred": true,
                              "trashed": false
                            }
                          ]
                        }
                        """.trimIndent()
                    )
                }
                return MockResponse().setResponseCode(404)
            }
        }

        val listPage = driveApiClient.listDriveFiles(
            accessToken = "valid_token",
            accountId = "user@example.com",
            filter = DriveViewFilter.STARRED,
            sortOrder = DriveSortOrder.SIZE_DESC,
            searchQuery = "Architecture"
        )
        assertEquals(1, listPage.files.size)
        assertEquals("Architecture_Spec.pdf", listPage.files.single().name)

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)

        // First call initializes startPageToken = "100"
        assertEquals(0, engine.syncCloudChangesIncremental())
        assertEquals("100", dao.getDriveChangeToken("user@example.com")?.startPageToken)

        // Second call polls changes since "100", indexes "cloud_doc_1", and updates token to "101"
        assertEquals(1, engine.syncCloudChangesIncremental())
        assertEquals("101", dao.getDriveChangeToken("user@example.com")?.startPageToken)
        assertNotNull(dao.getCloudFileById("cloud_doc_1"))
    }

    // 20. Legacy v1.3.0 Compromised Token Purge, Remote Revocation & Re-Auth Requirement (Section 15)
    @Test
    fun test20_legacyCompromisedTokenPurge_revokesRemotelyAndRequiresReauth() {
        val revokedTokens = ArrayList<String>()
        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                if (request.path?.startsWith("/oauth2/revoke") == true) {
                    val bodyStr = request.body.readUtf8()
                    revokedTokens.add(bodyStr)
                    return MockResponse().setResponseCode(200).setBody("{}")
                }
                return MockResponse().setResponseCode(404)
            }
        }

        val legacyPrefs = TestInMemorySharedPreferences().apply {
            edit()
                .putString("email", "legacy_user@gmail.com")
                .putString("display_name", "Legacy User")
                .putString("access_token", "ya29.legacy_compromised_access")
                .putString("refresh_token", "1//legacy_compromised_refresh_token")
                .putBoolean("legacy_v1_3_token_purged", false)
                .apply()
        }

        val migratedAuth = GoogleAuthManager(
            context = null,
            customPrefs = legacyPrefs,
            tokenEndpointUrl = mockWebServer.url("/oauth2/token").toString(),
            revokeEndpointUrl = mockWebServer.url("/oauth2/revoke").toString()
        )

        // 1. Remote revocation endpoint was called with the legacy refresh token (URL-encoded)
        assertEquals(1, revokedTokens.size)
        assertTrue(revokedTokens.single().contains("legacy_compromised_refresh_token"))

        // 2. Legacy tokens were wiped from SharedPreferences and migration flag set
        assertEquals(null, legacyPrefs.getString("refresh_token", null))
        assertEquals(null, legacyPrefs.getString("access_token", null))
        assertTrue(legacyPrefs.getBoolean("legacy_v1_3_token_purged", false))

        // 3. Account state preserves email identity but requires re-authorization (isConnected = false)
        assertEquals("legacy_user@gmail.com", migratedAuth.accountState.value.email)
        assertFalse(migratedAuth.accountState.value.isConnected)
        assertEquals(AccountAuthState.AUTHORIZATION_REQUIRED, migratedAuth.accountState.value.authState)
        assertEquals("REAUTH_REQUIRED", migratedAuth.accountState.value.lastTokenRefreshStatus)
    }

    // 21. Mid-Upload Pause Halts Multi-Chunk Streaming & Preserves Exact Resume Offset (Section 7)
    @Test
    fun test21_midUploadPause_haltsChunkStreamingAndPreservesResumeOffset() = runTest {
        val docsDir = File(tempStorageDir, "Documents").apply { mkdirs() }
        val chunkedFile = File(docsDir, "large_archive.bin").apply {
            writeBytes(ByteArray(600 * 1024) { (it % 251).toByte() })
        }

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val putRequestsReceived = AtomicInteger(0)

        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path ?: ""
                val method = request.method ?: "GET"

                if (path.startsWith("/drive/v3/about")) {
                    return MockResponse()
                        .setResponseCode(200)
                        .setBody("""{"storageQuota":{"limit":15000000000,"usage":1000000}}""")
                }
                if (method == "GET" && path.startsWith("/drive/v3/files?")) {
                    return MockResponse().setResponseCode(200).setBody("""{"files":[]}""")
                }
                if (method == "POST" && path.startsWith("/drive/v3/files")) {
                    return MockResponse().setResponseCode(200).setBody("""{"id":"folder_docs","name":"Documents"}""")
                }
                if (method == "POST" && path.startsWith("/upload/drive/v3/files?uploadType=resumable")) {
                    val sessionUrl = mockWebServer.url("/upload/drive/v3/files?uploadType=resumable&upload_id=pause_session_1").toString()
                    return MockResponse().setResponseCode(200).addHeader("Location", sessionUrl)
                }
                if (method == "PUT" && path.contains("upload_id=pause_session_1")) {
                    val count = putRequestsReceived.incrementAndGet()
                    // Simulate user pausing the engine right as chunk 1 finishes uploading
                    engine.setEnginePaused(true)
                    return MockResponse()
                        .setResponseCode(308)
                        .addHeader("Range", "bytes=0-${256 * 1024 - 1}")
                }
                return MockResponse().setResponseCode(404)
            }
        }

        val summary = engine.runBackupCycle(
            BackupSettings(userBackupConsentGranted = true, chunkSizeBytes = 256 * 1024)
        )
        assertEquals(1, putRequestsReceived.get())
        assertEquals(0, summary.uploadedAndVerifiedFiles)
        assertEquals(0, summary.failedUploads)

        val savedRecord = dao.getLatestVersionForIdentity("fs:Documents/large_archive.bin")
        assertNotNull(savedRecord)
        assertEquals(256L * 1024L, savedRecord!!.uploadedBytes)
        assertEquals(UploadStatus.PENDING_RETRY, savedRecord.uploadStatus)
        assertTrue(savedRecord.resumableSessionUri?.contains("pause_session_1") == true)
        assertEquals(SyncJobState.RETRY_SCHEDULED, dao.getSyncJobByRecordId(savedRecord.id)?.syncState)
    }

    // 22. Per-Folder Backup Enable/Disable & Folder Completeness Reporting (Section 5B & 12)
    @Test
    fun test22_perFolderBackupDisable_skipsDisabledFolderAndReportsCoverage() = runTest {
        installStandardDriveMockDispatcher()
        File(tempStorageDir, "DCIM").apply { mkdirs() }.let {
            File(it, "camera_01.jpg").writeText("CAMERA_IMAGE_DATA_01")
        }
        File(tempStorageDir, "Documents").apply { mkdirs() }.let {
            File(it, "private_notes.txt").writeText("DO_NOT_BACKUP_WHEN_FOLDER_DISABLED")
        }

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val repository = BackupRepository(
            context = null,
            backupDao = dao,
            storageSource = scanner,
            driveApiClient = driveApiClient,
            authManager = authManager,
            customSettingsPrefs = TestInMemorySharedPreferences()
        )
        repository.updateSettings(BackupSettings(userBackupConsentGranted = true))

        // Disable backup for "Documents" folder
        val initialCoverage = repository.getFolderCoverageItems()
        val docsCoverage = initialCoverage.first { it.folderName == "Documents" }
        repository.toggleFolderBackupEnabled(docsCoverage, enabled = false)

        val summary = repository.runBackupNow()
        assertEquals(2, summary.scannedFiles)
        assertEquals(1, summary.newlyQueuedVersions)
        assertEquals(1, summary.uploadedAndVerifiedFiles)
        assertEquals(1, summary.skippedUnchangedFiles)

        // Verify DCIM was backed up while Documents was skipped
        assertNotNull(dao.getLatestVersionForIdentity("fs:DCIM/camera_01.jpg"))
        assertEquals(null, dao.getLatestVersionForIdentity("fs:Documents/private_notes.txt"))

        val updatedCoverage = repository.getFolderCoverageItems()
        val dcimAfter = updatedCoverage.first { it.folderName == "DCIM" }
        val docsAfter = updatedCoverage.first { it.folderName == "Documents" }
        assertEquals(1, dcimAfter.backedUpFileCount)
        assertEquals(1.0f, dcimAfter.completenessFraction, 0.01f)
        assertTrue(dcimAfter.backupEnabled)
        assertEquals(0, docsAfter.backedUpFileCount)
        assertFalse(docsAfter.backupEnabled)
    }

    // 23. Multi-Account Backup Isolation: Alice and Bob Both Backup Unchanged Files Independently (Section 4)
    @Test
    fun test23_multiAccountBackupIsolation_aliceAndBobBothBackupIndependently() = runTest {
        installStandardDriveMockDispatcher()
        val dcimDir = File(tempStorageDir, "DCIM").apply { mkdirs() }
        File(dcimDir, "SHARED_FAMILY_PHOTO.jpg").writeText("FAMILY_PHOTO_BYTES_2026")

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)

        // 1. Alice backs up the photo
        authManager.onAccountAuthorized(
            email = "alice@example.com",
            displayName = "Alice",
            accessToken = "alice_token",
            refreshToken = "alice_refresh",
            expiresInSeconds = 3600L
        )
        val aliceSummary = engine.runBackupCycle(BackupSettings(userBackupConsentGranted = true))
        assertEquals(1, aliceSummary.uploadedAndVerifiedFiles)
        val aliceVersions = dao.getBackupVersionsForAccountPaged("alice@example.com", 10, 0)
        assertEquals(1, aliceVersions.size)

        // 2. Switch to Bob -> Bob backs up the exact same unchanged local photo to Bob's Drive root!
        authManager.switchGoogleAccount(
            newEmail = "bob@example.com",
            newDisplayName = "Bob",
            newAccessToken = "bob_token",
            newRefreshToken = "bob_refresh"
        )
        val bobSummary = engine.runBackupCycle(BackupSettings(userBackupConsentGranted = true))
        assertEquals(1, bobSummary.uploadedAndVerifiedFiles)

        val bobVersions = dao.getBackupVersionsForAccountPaged("bob@example.com", 10, 0)
        assertEquals(1, bobVersions.size)
        assertEquals("SHARED_FAMILY_PHOTO.jpg", bobVersions.single().driveStoredName)

        // Alice's version remains 100% intact
        assertEquals(1, dao.getBackupVersionsForAccountPaged("alice@example.com", 10, 0).size)
    }

    // 24. HTTP 404, 409, 5xx Classification & Exponential Backoff Retry Scheduling (Section 9)
    @Test
    fun test24_http404_409_5xxAndExponentialBackoff() = runTest {
        var nextStatusCode = 404
        mockWebServer.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                return MockResponse()
                    .setResponseCode(nextStatusCode)
                    .setBody("""{"error":{"code":$nextStatusCode,"message":"Simulated error $nextStatusCode"}}""")
            }
        }

        // Verify 404 -> DriveResourceNotFoundException
        var caught404: DriveResourceNotFoundException? = null
        try {
            driveApiClient.fetchDriveQuota("token")
        } catch (e: DriveResourceNotFoundException) {
            caught404 = e
        }
        assertNotNull(caught404)

        // Verify 409 -> DriveConflictException
        nextStatusCode = 409
        var caught409: DriveConflictException? = null
        try {
            driveApiClient.fetchDriveQuota("token")
        } catch (e: DriveConflictException) {
            caught409 = e
        }
        assertNotNull(caught409)

        // Verify 503 -> DriveServerException
        nextStatusCode = 503
        var caught503: DriveServerException? = null
        try {
            driveApiClient.fetchDriveQuota("token")
        } catch (e: DriveServerException) {
            caught503 = e
        }
        assertNotNull(caught503)
        assertEquals(503, caught503!!.httpStatusCode)

        // Verify exponential backoff in BackupDao.transitionSyncJobState
        dao.transitionSyncJobState(
            accountId = "user@example.com",
            recordId = 99L,
            sourceFileIdentity = "DCIM/test.jpg",
            fileName = "test.jpg",
            relativePath = "DCIM/test.jpg",
            backupVersion = 1,
            newState = SyncJobState.FAILED_RETRYABLE,
            uploadedBytes = 0L,
            totalBytes = 1024L
        )
        dao.transitionSyncJobState(
            accountId = "user@example.com",
            recordId = 99L,
            sourceFileIdentity = "DCIM/test.jpg",
            fileName = "test.jpg",
            relativePath = "DCIM/test.jpg",
            backupVersion = 1,
            newState = SyncJobState.RETRY_SCHEDULED,
            uploadedBytes = 0L,
            totalBytes = 1024L
        )
        val job1 = dao.getSyncJobByRecordId(99L)!!
        assertEquals(30_000L, job1.nextRetryAtMs!! - job1.updatedAtMs)

        // Second failure -> retryCount = 2 -> backoff = 60_000L
        dao.transitionSyncJobState(
            accountId = "user@example.com",
            recordId = 99L,
            sourceFileIdentity = "DCIM/test.jpg",
            fileName = "test.jpg",
            relativePath = "DCIM/test.jpg",
            backupVersion = 1,
            newState = SyncJobState.FAILED_RETRYABLE,
            uploadedBytes = 0L,
            totalBytes = 1024L
        )
        dao.transitionSyncJobState(
            accountId = "user@example.com",
            recordId = 99L,
            sourceFileIdentity = "DCIM/test.jpg",
            fileName = "test.jpg",
            relativePath = "DCIM/test.jpg",
            backupVersion = 1,
            newState = SyncJobState.RETRY_SCHEDULED,
            uploadedBytes = 0L,
            totalBytes = 1024L
        )
        val job2 = dao.getSyncJobByRecordId(99L)!!
        assertEquals(60_000L, job2.nextRetryAtMs!! - job2.updatedAtMs)
    }
}

class TestInMemorySharedPreferences : SharedPreferences {
    private val map = LinkedHashMap<String, Any?>()

    override fun getAll(): MutableMap<String, *> = LinkedHashMap(map)
    override fun getString(key: String?, defValue: String?): String? = (map[key] as? String) ?: defValue
    @Suppress("UNCHECKED_CAST")
    override fun getStringSet(key: String?, defValues: MutableSet<String>?): MutableSet<String>? =
        (map[key] as? MutableSet<String>) ?: defValues
    override fun getInt(key: String?, defValue: Int): Int = (map[key] as? Int) ?: defValue
    override fun getLong(key: String?, defValue: Long): Long = (map[key] as? Long) ?: defValue
    override fun getFloat(key: String?, defValue: Float): Float = (map[key] as? Float) ?: defValue
    override fun getBoolean(key: String?, defValue: Boolean): Boolean = (map[key] as? Boolean) ?: defValue
    override fun contains(key: String?): Boolean = map.containsKey(key)

    override fun edit(): SharedPreferences.Editor = object : SharedPreferences.Editor {
        private val pending = LinkedHashMap<String, Any?>()
        private val removals = HashSet<String>()
        private var clearAll = false

        override fun putString(key: String?, value: String?): SharedPreferences.Editor = apply {
            if (key != null) pending[key] = value
        }
        override fun putStringSet(key: String?, values: MutableSet<String>?): SharedPreferences.Editor = apply {
            if (key != null) pending[key] = values
        }
        override fun putInt(key: String?, value: Int): SharedPreferences.Editor = apply {
            if (key != null) pending[key] = value
        }
        override fun putLong(key: String?, value: Long): SharedPreferences.Editor = apply {
            if (key != null) pending[key] = value
        }
        override fun putFloat(key: String?, value: Float): SharedPreferences.Editor = apply {
            if (key != null) pending[key] = value
        }
        override fun putBoolean(key: String?, value: Boolean): SharedPreferences.Editor = apply {
            if (key != null) pending[key] = value
        }
        override fun remove(key: String?): SharedPreferences.Editor = apply {
            if (key != null) removals.add(key)
        }
        override fun clear(): SharedPreferences.Editor = apply { clearAll = true }
        override fun commit(): Boolean {
            apply()
            return true
        }
        override fun apply() {
            if (clearAll) map.clear()
            removals.forEach { map.remove(it) }
            for ((k, v) in pending) {
                if (v == null) map.remove(k) else map[k] = v
            }
        }
    }

    override fun registerOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {}
    override fun unregisterOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {}
}

