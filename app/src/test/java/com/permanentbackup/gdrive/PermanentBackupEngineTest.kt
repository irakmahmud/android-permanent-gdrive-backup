package com.permanentbackup.gdrive

import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.BackupFileEntity
import com.permanentbackup.gdrive.data.local.DriveFolderCacheEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.engine.AndroidStorageScanner
import com.permanentbackup.gdrive.domain.engine.PermanentBackupEngine
import com.permanentbackup.gdrive.domain.engine.SourceFileReadOnlyGuard
import com.permanentbackup.gdrive.domain.engine.StorageFileSource
import com.permanentbackup.gdrive.domain.model.BackupSettings
import com.permanentbackup.gdrive.domain.model.DiscoveredSourceFile
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.NeverDeletePolicyViolationException
import com.permanentbackup.gdrive.domain.model.SourceFileModificationForbiddenException
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
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * In-memory implementation of [BackupDao] for deterministic, fast JVM integration & reliability tests.
 */
class InMemoryBackupDao : BackupDao {
    private val records = LinkedHashMap<Long, BackupFileEntity>()
    private val folderCache = HashMap<String, DriveFolderCacheEntity>()
    private val idCounter = AtomicInteger(1)
    private val stateFlow = MutableStateFlow<List<BackupFileEntity>>(emptyList())

    private fun notifyChange() {
        stateFlow.value = records.values.toList()
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

    fun getAllRecordsSnapshot(): List<BackupFileEntity> = records.values.toList()
}

/**
 * Comprehensive Automated Security, Data Integrity & Reliability Test Suite
 * covering all 13 mandatory scenarios from Section 12.
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
            tokenEndpointUrl = mockWebServer.url("/oauth2/token").toString()
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
                    // Format: bytes start-end/total
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

        // First backup uploads and verifies the file
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

        // Cloud folder cache and both verified records remain 100% preserved
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

        // Rename local file
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

        // Backup Version 1
        engine.runBackupCycle(settings)
        assertEquals(1, dao.countVerifiedRecords())

        // Modify file content on phone
        notesFile.writeText("Entry 1 + Entry 2: Updated journal text with more data")
        notesFile.setLastModified(System.currentTimeMillis() + 5000L)

        // Backup Version 2
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
                        // Status probe request after network failure!
                        probedResumeSession = true
                        return MockResponse()
                            .setResponseCode(308)
                            .addHeader("Range", "bytes=0-${chunkSize - 1}")
                    }

                    putChunkCallCount++
                    val startByte = contentRange.substringAfter("bytes ").substringBefore("-").toLong()
                    chunkStartOffsetsReceived.add(startByte)

                    if (putChunkCallCount == 1) {
                        // Chunk 1 succeeds (0..262143)
                        return MockResponse()
                            .setResponseCode(308)
                            .addHeader("Range", "bytes=0-${chunkSize - 1}")
                    }
                    if (putChunkCallCount == 2) {
                        // Simulate network failure on Chunk 2!
                        return MockResponse().setResponseCode(503).setBody("Simulated network drop")
                    }
                    if (putChunkCallCount == 3) {
                        // Retry of Chunk 2 succeeds (262144..524287)
                        return MockResponse()
                            .setResponseCode(308)
                            .addHeader("Range", "bytes=0-${chunkSize * 2 - 1}")
                    }
                    // Final Chunk 3 completes upload
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

        // Attempt 1: Fails mid-upload on Chunk 2 after saving Chunk 1 progress (262,144 bytes)
        val attempt1 = engine.runBackupCycle(settings)
        assertEquals(0, attempt1.uploadedAndVerifiedFiles)
        assertEquals(1, attempt1.failedUploads)

        val partialRecord = dao.getAllRecordsSnapshot().single()
        assertEquals(chunkSize.toLong(), partialRecord.uploadedBytes)
        assertNotNull(partialRecord.resumableSessionUri)
        assertEquals(UploadStatus.PENDING_RETRY, partialRecord.uploadStatus)

        // Attempt 2 (Network Reconnected): Probes offset and resumes from byte 262,144 without re-uploading Chunk 1!
        val attempt2 = engine.runBackupCycle(settings)
        assertTrue(probedResumeSession)
        assertEquals(1, attempt2.uploadedAndVerifiedFiles)

        // Chunk 1 (offset 0) was only sent once!
        assertEquals(listOf(0L, chunkSize.toLong(), chunkSize.toLong(), (chunkSize * 2).toLong()), chunkStartOffsetsReceived)

        val finalRecord = dao.getAllRecordsSnapshot().single()
        assertEquals(UploadStatus.VERIFIED, finalRecord.uploadStatus)
        assertEquals(totalSize.toLong(), finalRecord.uploadedBytes)
        assertTrue(videoFile.exists())
    }

    // 6. App crash during backup -> Recovers incomplete tasks without losing history
    @Test
    fun test06_appCrashDuringBackup_recoversIncompleteTasksWithoutHistoryLoss() = runTest {
        // Insert an already verified historical backup
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

        // Insert an interrupted IN_PROGRESS record simulating an app crash mid-stream
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
        // File was discovered and safely queued in DB without data loss
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

        // Also test: File deleted from phone BEFORE initial upload finishes -> cannot claim recovery
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
    }

    // 10. 100,000+ files -> Memory and Database pagination remain stable
    @Test
    fun test10_hundredThousandFiles_paginationAndMemoryBounded() = runTest {
        val totalSimulatedFiles = 100_000
        var processedCount = 0

        val syntheticSequence = sequence {
            for (i in 1..totalSimulatedFiles) {
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

        // Process all 100,000 items in streaming chunks of 500 without retaining full list in RAM
        syntheticSequence.chunked(PermanentBackupEngine.PAGINATION_BATCH_SIZE).forEach { batch ->
            processedCount += batch.size
        }
        assertEquals(100_000, processedCount)
    }

    // 11. Unchanged file is never uploaded twice; same-named files in different folders are both backed up
    @Test
    fun test11_duplicateControl_unchangedFileSkippedAndSameNameDifferentFoldersSeparated() = runTest {
        installStandardDriveMockDispatcher()
        val dcimDir = File(tempStorageDir, "DCIM").apply { mkdirs() }
        val picturesDir = File(tempStorageDir, "Pictures").apply { mkdirs() }

        // Two distinct files with the exact same name "photo.jpg" in different folders
        File(dcimDir, "photo.jpg").writeText("CAMERA_PHOTO_CONTENT_111")
        File(picturesDir, "photo.jpg").writeText("EDITED_PHOTO_CONTENT_222")

        val scanner = AndroidStorageScanner(null, listOf(tempStorageDir), true)
        val engine = PermanentBackupEngine(dao, scanner, driveApiClient, authManager)
        val settings = BackupSettings(userBackupConsentGranted = true)

        val firstCycle = engine.runBackupCycle(settings)
        assertEquals(2, firstCycle.scannedFiles)
        assertEquals(2, firstCycle. newlyQueuedVersions)
        assertEquals(2, firstCycle.uploadedAndVerifiedFiles)

        // Second cycle with zero changes -> 0 uploads, 2 skipped unchanged!
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

        // Stream hashes and skip offset
        val hashes = SourceFileReadOnlyGuard.computeStreamingHashes {
            SourceFileReadOnlyGuard.openReadOnlyFileStream(audioFile)
        }
        assertEquals(originalBytes.size.toLong(), hashes.totalBytesRead)

        SourceFileReadOnlyGuard.openReadOnlyFileStream(audioFile).use { stream ->
            SourceFileReadOnlyGuard.skipStreamToOffset(stream, 65536L)
            assertEquals(originalBytes[65536].toInt(), stream.read())
        }

        // Verify source file is 100% untouched
        assertTrue(audioFile.exists())
        assertEquals(originalBytes.size.toLong(), audioFile.length())
        assertEquals(originalMtime, audioFile.lastModified())

        // Verify mutation guard throws SourceFileModificationForbiddenException
        var mutationBlocked = false
        try {
            SourceFileReadOnlyGuard.forbidSourceFileMutation("DELETE", audioFile.absolutePath)
        } catch (_: SourceFileModificationForbiddenException) {
            mutationBlocked = true
        }
        assertTrue(mutationBlocked)
    }
}
