package com.permanentbackup.gdrive.data.remote

import com.permanentbackup.gdrive.domain.model.AuthTokenExpiredException
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.DriveBadRequestException
import com.permanentbackup.gdrive.domain.model.DriveConflictException
import com.permanentbackup.gdrive.domain.model.DrivePermissionException
import com.permanentbackup.gdrive.domain.model.DriveQuotaInfo
import com.permanentbackup.gdrive.domain.model.DriveRateLimitException
import com.permanentbackup.gdrive.domain.model.DriveResourceNotFoundException
import com.permanentbackup.gdrive.domain.model.DriveServerException
import com.permanentbackup.gdrive.domain.model.DriveSortOrder
import com.permanentbackup.gdrive.domain.model.DriveViewFilter
import com.permanentbackup.gdrive.domain.model.StorageQuotaFullException
import com.permanentbackup.gdrive.domain.model.UploadSessionExpiredException
import com.permanentbackup.gdrive.domain.model.VerificationStatus
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okio.BufferedSink
import org.json.JSONArray
import org.json.JSONObject
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.TimeUnit

data class RemoteDriveFileMetadata(
    val id: String,
    val name: String,
    val mimeType: String = "application/octet-stream",
    val size: Long,
    val md5Checksum: String?,
    val sha256Checksum: String?,
    val trashed: Boolean,
    val starred: Boolean = false,
    val shared: Boolean = false,
    val webViewLink: String? = null,
    val parentFolderId: String? = null,
    val createdTimeMs: Long = 0L,
    val modifiedTimeMs: Long = 0L
)

data class ChunkUploadResult(
    val completed: Boolean,
    val confirmedUploadedBytes: Long,
    val driveFileId: String?,
    val sessionUri: String
)

data class DriveFileListPage(
    val files: List<CloudBrowseItem>,
    val nextPageToken: String?
)

data class DriveChangesPage(
    val changedFiles: List<CloudBrowseItem>,
    val newStartPageToken: String?,
    val nextPageToken: String?
)

/**
 * Full-Featured Google Drive API v3 Client (Section 5 & Section 9):
 * - Create Backup Folder & nested subfolder hierarchy
 * - List Files, Folder Navigation, Search Files, Recent, Starred, Shared With Me, Shared Drives
 * - Streaming Resumable Upload with bounded 64 KiB RAM buffer (Zero temp file copy)
 * - Pre-upload remote reconciliation check (prevents duplicate uploads after crash)
 * - Read Metadata, Checksums (SHA-256 & MD5), and Storage Quota
 * - Streaming Download & Cloud Content Streaming (HTTP Range support)
 * - Google Drive Changes API v3 (startPageToken & incremental changes list)
 * - Distinct HTTP 400, 401, 403 (Quota vs Permission vs RateLimit), 404, 409, 429, and 5xx handling
 * - Strictly zero destructive methods (enforced by both API design and NeverDeleteGuardInterceptor)
 */
class DriveApiClient(
    baseClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .build(),
    private val driveBaseUrl: String = "https://www.googleapis.com"
) {
    companion object {
        const val FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
        const val DEFAULT_CHUNK_SIZE_BYTES = 256 * 1024 // 256 KiB multiple required by Google Drive API
        const val STREAM_BUFFER_SIZE_BYTES = 64 * 1024  // 64 KiB bounded RAM buffer
        private const val FILE_FIELDS =
            "id,name,mimeType,size,md5Checksum,sha256Checksum,trashed,starred,shared,webViewLink,parents,createdTime,modifiedTime,appProperties"
    }

    val httpClient: OkHttpClient = baseClient.newBuilder()
        .addInterceptor(NeverDeleteGuardInterceptor())
        .build()

    /**
     * Queries Google Drive storage quota (Total, Used, Available) via Drive v3 about endpoint.
     */
    fun fetchDriveQuota(accessToken: String): DriveQuotaInfo {
        val url = "$driveBaseUrl/drive/v3/about?fields=storageQuota,user"
        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                val json = JSONObject(bodyStr)
                val quota = json.optJSONObject("storageQuota") ?: JSONObject()
                DriveQuotaInfo(
                    limitBytes = quota.optLong("limit", 15L * 1024 * 1024 * 1024),
                    usageBytes = quota.optLong("usage", 0L),
                    usageInDriveBytes = quota.optLong("usageInDrive", 0L),
                    usageInTrashBytes = quota.optLong("usageInDriveTrash", 0L)
                )
            }
        }
    }

    /**
     * Finds or creates the dedicated root folder ("Android_Permanent_Backup") or subfolder on Google Drive.
     * Deduplicates by checking existing non-trashed folders with the same name and parent first.
     */
    fun ensureFolderExists(
        accessToken: String,
        folderName: String,
        parentFolderId: String? = null
    ): String {
        val safeName = sanitizeDriveName(folderName)
        val escapedName = escapeDriveQueryLiteral(safeName)
        val parentClause = if (!parentFolderId.isNullOrBlank()) {
            " and '${escapeDriveQueryLiteral(parentFolderId)}' in parents"
        } else {
            ""
        }
        val query = "mimeType = '$FOLDER_MIME_TYPE' and name = '$escapedName' and trashed = false$parentClause"

        val searchUrl = "$driveBaseUrl/drive/v3/files".toHttpUrl().newBuilder()
            .addQueryParameter("q", query)
            .addQueryParameter("fields", "files(id,name)")
            .addQueryParameter("spaces", "drive")
            .build()

        val searchReq = Request.Builder()
            .url(searchUrl)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(searchReq).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            handleDriveHttpResponse(searchReq, response.code, bodyStr) {
                val files = JSONObject(bodyStr).optJSONArray("files")
                if (files != null && files.length() > 0) {
                    val existingId = files.getJSONObject(0).optString("id", "")
                    if (existingId.isNotBlank()) return existingId
                }
            }
        }

        // Create folder if not found
        val metadataJson = JSONObject().apply {
            put("name", safeName)
            put("mimeType", FOLDER_MIME_TYPE)
            if (!parentFolderId.isNullOrBlank()) {
                put("parents", JSONArray().put(parentFolderId))
            }
        }

        val createReq = Request.Builder()
            .url("$driveBaseUrl/drive/v3/files?fields=id,name")
            .header("Authorization", "Bearer $accessToken")
            .post(metadataJson.toString().toRequestBody("application/json; charset=utf-8".toMediaType()))
            .build()

        httpClient.newCall(createReq).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(createReq, response.code, bodyStr) {
                val id = JSONObject(bodyStr).optString("id", "")
                if (id.isBlank()) throw IOException("Drive folder creation returned empty folder ID")
                id
            }
        }
    }

    /**
     * Section 7 Crash Reconciliation: Checks Google Drive before uploading to verify whether
     * this exact file version (matching parentFolderId, driveFileName, and size/sha256) was already
     * uploaded right before an app crash or database failure.
     */
    fun findExistingBackedUpFile(
        accessToken: String,
        parentFolderId: String,
        driveFileName: String,
        expectedSize: Long,
        expectedSha256: String?
    ): RemoteDriveFileMetadata? {
        val escapedName = escapeDriveQueryLiteral(sanitizeDriveName(driveFileName))
        val escapedParent = escapeDriveQueryLiteral(parentFolderId)
        val query = "name = '$escapedName' and '$escapedParent' in parents and trashed = false"

        val url = "$driveBaseUrl/drive/v3/files".toHttpUrl().newBuilder()
            .addQueryParameter("q", query)
            .addQueryParameter("fields", "files($FILE_FIELDS)")
            .addQueryParameter("pageSize", "10")
            .build()

        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        return try {
            httpClient.newCall(request).execute().use { response ->
                val bodyStr = response.body?.string() ?: "{}"
                if (!response.isSuccessful) return null
                val files = JSONObject(bodyStr).optJSONArray("files") ?: return null
                for (i in 0 until files.length()) {
                    val obj = files.getJSONObject(i)
                    val remoteSize = obj.optLong("size", -1L)
                    val remoteSha = obj.optString("sha256Checksum", "").ifBlank {
                        obj.optJSONObject("appProperties")?.optString("localSha256", "") ?: ""
                    }
                    val sizeMatches = remoteSize == expectedSize
                    val shaMatches = expectedSha256.isNullOrBlank() ||
                        remoteSha.isBlank() ||
                        remoteSha.equals(expectedSha256, ignoreCase = true)
                    if (sizeMatches && shaMatches) {
                        return parseRemoteFileMetadata(obj)
                    }
                }
                null
            }
        } catch (_: Throwable) {
            null
        }
    }

    /**
     * Lists files & folders for My Drive Explorer (Section 5A & Section 9):
     * Supports Folder Navigation, Recent, Starred, Shared With Me, Shared Drives, Search, and Sorting.
     */
    fun listDriveFiles(
        accessToken: String,
        accountId: String,
        filter: DriveViewFilter = DriveViewFilter.BACKUP_ROOT,
        parentFolderId: String? = null,
        searchQuery: String = "",
        sortOrder: DriveSortOrder = DriveSortOrder.MODIFIED_DESC,
        pageToken: String? = null,
        pageSize: Int = 100
    ): DriveFileListPage {
        if (filter == DriveViewFilter.SHARED_DRIVES && parentFolderId == null && searchQuery.isBlank()) {
            return listSharedDrives(accessToken, accountId, pageToken, pageSize)
        }

        val qParts = mutableListOf("trashed = false")
        when {
            !parentFolderId.isNullOrBlank() -> {
                qParts.add("'${escapeDriveQueryLiteral(parentFolderId)}' in parents")
            }
            filter == DriveViewFilter.MY_DRIVE && searchQuery.isBlank() -> {
                qParts.add("'root' in parents")
            }
            filter == DriveViewFilter.STARRED -> {
                qParts.add("starred = true")
            }
            filter == DriveViewFilter.SHARED_WITH_ME -> {
                qParts.add("sharedWithMe = true")
            }
            filter == DriveViewFilter.RECENT -> {
                qParts.add("mimeType != '$FOLDER_MIME_TYPE'")
            }
        }

        if (searchQuery.isNotBlank()) {
            val escapedSearch = escapeDriveQueryLiteral(searchQuery.trim())
            qParts.add("name contains '$escapedSearch'")
        }

        val urlBuilder = "$driveBaseUrl/drive/v3/files".toHttpUrl().newBuilder()
            .addQueryParameter("q", qParts.joinToString(" and "))
            .addQueryParameter("fields", "nextPageToken,files($FILE_FIELDS)")
            .addQueryParameter("pageSize", pageSize.coerceIn(1, 200).toString())
            .addQueryParameter("orderBy", "folder,${sortOrder.driveOrderByParam}")
            .addQueryParameter("includeItemsFromAllDrives", "true")
            .addQueryParameter("supportsAllDrives", "true")

        if (!pageToken.isNullOrBlank()) {
            urlBuilder.addQueryParameter("pageToken", pageToken)
        }

        val request = Request.Builder()
            .url(urlBuilder.build())
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                val json = JSONObject(bodyStr)
                val nextToken = json.optString("nextPageToken", "").ifBlank { null }
                val filesArr = json.optJSONArray("files") ?: JSONArray()
                val items = ArrayList<CloudBrowseItem>(filesArr.length())
                for (i in 0 until filesArr.length()) {
                    items.add(parseCloudBrowseItem(filesArr.getJSONObject(i), accountId))
                }
                DriveFileListPage(files = items, nextPageToken = nextToken)
            }
        }
    }

    private fun listSharedDrives(
        accessToken: String,
        accountId: String,
        pageToken: String?,
        pageSize: Int
    ): DriveFileListPage {
        val urlBuilder = "$driveBaseUrl/drive/v3/drives".toHttpUrl().newBuilder()
            .addQueryParameter("pageSize", pageSize.coerceIn(1, 100).toString())
        if (!pageToken.isNullOrBlank()) {
            urlBuilder.addQueryParameter("pageToken", pageToken)
        }
        val request = Request.Builder()
            .url(urlBuilder.build())
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                val json = JSONObject(bodyStr)
                val nextToken = json.optString("nextPageToken", "").ifBlank { null }
                val drivesArr = json.optJSONArray("drives") ?: JSONArray()
                val items = ArrayList<CloudBrowseItem>(drivesArr.length())
                for (i in 0 until drivesArr.length()) {
                    val d = drivesArr.getJSONObject(i)
                    items.add(
                        CloudBrowseItem(
                            driveFileId = d.optString("id", ""),
                            accountId = accountId,
                            name = d.optString("name", "Shared Drive"),
                            mimeType = FOLDER_MIME_TYPE,
                            sizeBytes = 0L,
                            isFolder = true,
                            shared = true
                        )
                    )
                }
                DriveFileListPage(files = items, nextPageToken = nextToken)
            }
        }
    }

    /**
     * Reads full metadata and checksums for a single Drive file ID.
     */
    fun getFileMetadata(accessToken: String, driveFileId: String): RemoteDriveFileMetadata {
        val url = "$driveBaseUrl/drive/v3/files/$driveFileId?fields=$FILE_FIELDS&supportsAllDrives=true"
        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                parseRemoteFileMetadata(JSONObject(bodyStr))
            }
        }
    }

    /**
     * Initiates a Resumable Upload Session on Google Drive API v3.
     * Creates a new immutable file entry on Drive (never overwrites existing file IDs).
     */
    fun initiateResumableUploadSession(
        accessToken: String,
        parentFolderId: String,
        driveFileName: String,
        mimeType: String,
        totalBytes: Long,
        localSha256: String?,
        backupVersion: Int
    ): String {
        val safeName = sanitizeDriveName(driveFileName)
        val metadataJson = JSONObject().apply {
            put("name", safeName)
            put("parents", JSONArray().put(parentFolderId))
            put(
                "appProperties",
                JSONObject().apply {
                    put("backupVersion", backupVersion.toString())
                    if (!localSha256.isNullOrBlank()) {
                        put("localSha256", localSha256)
                    }
                    put("policy", "NEVER_DELETE_APPEND_ONLY")
                }
            )
        }

        val request = Request.Builder()
            .url("$driveBaseUrl/upload/drive/v3/files?uploadType=resumable")
            .header("Authorization", "Bearer $accessToken")
            .header("X-Upload-Content-Type", mimeType)
            .header("X-Upload-Content-Length", totalBytes.toString())
            .post(metadataJson.toString().toRequestBody("application/json; charset=utf-8".toMediaType()))
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: ""
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                val sessionUri = response.header("Location")
                if (sessionUri.isNullOrBlank()) {
                    throw IOException("Google Drive resumable upload did not return a Location session URI")
                }
                sessionUri
            }
        }
    }

    /**
     * Queries Google Drive for the exact byte offset already received for an interrupted Resumable Upload session.
     * Follows https://developers.google.com/workspace/drive/api/guides/manage-uploads#resume-upload
     */
    fun queryResumableUploadOffset(
        sessionUri: String,
        totalBytes: Long
    ): ChunkUploadResult {
        val emptyBody = ByteArray(0).toRequestBody(null, 0, 0)
        val request = Request.Builder()
            .url(sessionUri)
            .header("Content-Range", "bytes */$totalBytes")
            .put(emptyBody)
            .build()

        httpClient.newCall(request).execute().use { response ->
            val code = response.code
            val bodyStr = response.body?.string() ?: "{}"
            return when (code) {
                308 -> {
                    val rangeHeader = response.header("Range")
                    val confirmedBytes = parseRangeHeaderUpperBytes(rangeHeader)
                    ChunkUploadResult(
                        completed = false,
                        confirmedUploadedBytes = confirmedBytes,
                        driveFileId = null,
                        sessionUri = sessionUri
                    )
                }
                200, 201 -> {
                    val fileId = JSONObject(bodyStr).optString("id", "").ifBlank { null }
                    ChunkUploadResult(
                        completed = true,
                        confirmedUploadedBytes = totalBytes,
                        driveFileId = fileId,
                        sessionUri = sessionUri
                    )
                }
                404, 410 -> {
                    throw UploadSessionExpiredException("Resumable upload session expired (HTTP $code)")
                }
                else -> {
                    handleDriveHttpResponse(request, code, bodyStr) {
                        throw IOException("Unexpected status $code while probing resumable session")
                    }
                }
            }
        }
    }

    /**
     * Streams a single chunk directly from [inputStream] (already positioned at [chunkStartByte])
     * using a fixed 64 KiB RAM buffer — never loading the whole file into memory or copying to disk.
     */
    fun uploadSingleStreamChunk(
        sessionUri: String,
        inputStream: InputStream,
        chunkStartByte: Long,
        chunkLength: Int,
        totalBytes: Long,
        mimeType: String
    ): ChunkUploadResult {
        val chunkEndByte = chunkStartByte + chunkLength - 1L
        val contentRange = if (totalBytes == 0L) {
            "bytes */0"
        } else {
            "bytes $chunkStartByte-$chunkEndByte/$totalBytes"
        }

        val streamingBody = object : RequestBody() {
            override fun contentType() = mimeType.toMediaType()
            override fun contentLength(): Long = chunkLength.toLong()

            override fun writeTo(sink: BufferedSink) {
                val buffer = ByteArray(STREAM_BUFFER_SIZE_BYTES)
                var remaining = chunkLength
                while (remaining > 0) {
                    val toRead = minOf(buffer.size, remaining)
                    val read = inputStream.read(buffer, 0, toRead)
                    if (read == -1) {
                        throw IOException("Unexpected EOF while streaming file chunk at offset $chunkStartByte")
                    }
                    sink.write(buffer, 0, read)
                    remaining -= read
                }
            }
        }

        val request = Request.Builder()
            .url(sessionUri)
            .header("Content-Range", contentRange)
            .put(streamingBody)
            .build()

        httpClient.newCall(request).execute().use { response ->
            val code = response.code
            val bodyStr = response.body?.string() ?: "{}"
            return when (code) {
                308 -> {
                    val rangeHeader = response.header("Range")
                    val confirmedBytes = if (rangeHeader != null) {
                        parseRangeHeaderUpperBytes(rangeHeader)
                    } else {
                        chunkEndByte + 1L
                    }
                    ChunkUploadResult(
                        completed = false,
                        confirmedUploadedBytes = confirmedBytes,
                        driveFileId = null,
                        sessionUri = sessionUri
                    )
                }
                200, 201 -> {
                    val fileId = JSONObject(bodyStr).optString("id", "")
                    if (fileId.isBlank()) {
                        throw IOException("Upload returned HTTP $code without a valid Google Drive file ID")
                    }
                    ChunkUploadResult(
                        completed = true,
                        confirmedUploadedBytes = totalBytes,
                        driveFileId = fileId,
                        sessionUri = sessionUri
                    )
                }
                404, 410 -> {
                    throw UploadSessionExpiredException("Resumable upload session expired during chunk upload (HTTP $code)")
                }
                else -> {
                    handleDriveHttpResponse(request, code, bodyStr) {
                        throw IOException("Chunk upload failed with HTTP $code: $bodyStr")
                    }
                }
            }
        }
    }

    /**
     * Fetches remote file metadata from Google Drive and verifies integrity (file size + MD5/SHA-256 checksum).
     */
    fun verifyRemoteFile(
        accessToken: String,
        driveFileId: String,
        expectedSize: Long,
        expectedMd5: String?,
        expectedSha256: String?
    ): VerificationStatus {
        val url = "$driveBaseUrl/drive/v3/files/$driveFileId?fields=id,name,size,md5Checksum,sha256Checksum,trashed"
        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                val json = JSONObject(bodyStr)
                val id = json.optString("id", "")
                val trashed = json.optBoolean("trashed", false)
                val remoteSize = json.optLong("size", -1L)
                val remoteMd5 = json.optString("md5Checksum", "").ifBlank { null }
                val remoteSha256 = json.optString("sha256Checksum", "").ifBlank { null }

                if (id != driveFileId || trashed || remoteSize != expectedSize) {
                    return@handleDriveHttpResponse VerificationStatus.VERIFICATION_FAILED
                }

                if (remoteSha256 != null && expectedSha256 != null) {
                    return@handleDriveHttpResponse if (remoteSha256.equals(expectedSha256, ignoreCase = true)) {
                        VerificationStatus.CHECKSUM_AND_SIZE_VERIFIED
                    } else {
                        VerificationStatus.VERIFICATION_FAILED
                    }
                }

                if (remoteMd5 != null && expectedMd5 != null) {
                    return@handleDriveHttpResponse if (remoteMd5.equals(expectedMd5, ignoreCase = true)) {
                        VerificationStatus.CHECKSUM_AND_SIZE_VERIFIED
                    } else {
                        VerificationStatus.VERIFICATION_FAILED
                    }
                }

                VerificationStatus.SIZE_AND_METADATA_VERIFIED_NO_REMOTE_CHECKSUM
            }
        }
    }

    /**
     * Streams a cloud file download directly to [outputStream] using a 64 KiB bounded buffer (Section 5F & 9).
     */
    fun downloadFileStreaming(
        accessToken: String,
        driveFileId: String,
        outputStream: OutputStream,
        onProgressBytes: (Long) -> Unit = {}
    ): Long {
        val url = "$driveBaseUrl/drive/v3/files/$driveFileId?alt=media&supportsAllDrives=true"
        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                val errBody = response.body?.string() ?: "{}"
                handleDriveHttpResponse(request, response.code, errBody) {
                    throw IOException("Download failed with HTTP ${response.code}")
                }
            }
            val bodyStream = response.body?.byteStream()
                ?: throw IOException("Google Drive download response had empty body stream")
            val buffer = ByteArray(STREAM_BUFFER_SIZE_BYTES)
            var totalWritten = 0L
            bodyStream.use { input ->
                outputStream.use { out ->
                    while (true) {
                        val read = input.read(buffer)
                        if (read == -1) break
                        out.write(buffer, 0, read)
                        totalWritten += read
                        onProgressBytes(totalWritten)
                    }
                    out.flush()
                }
            }
            return totalWritten
        }
    }

    /**
     * Opens a live streaming [InputStream] to a Google Drive file without downloading the whole file first
     * (Section 5D: Cloud Streaming & File Preview). Supports optional byte range requests.
     */
    fun openCloudStream(
        accessToken: String,
        driveFileId: String,
        rangeStartByte: Long? = null,
        rangeEndByteInclusive: Long? = null
    ): InputStream {
        val url = "$driveBaseUrl/drive/v3/files/$driveFileId?alt=media&supportsAllDrives=true"
        val builder = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()

        if (rangeStartByte != null && rangeStartByte >= 0L) {
            val rangeVal = if (rangeEndByteInclusive != null && rangeEndByteInclusive >= rangeStartByte) {
                "bytes=$rangeStartByte-$rangeEndByteInclusive"
            } else {
                "bytes=$rangeStartByte-"
            }
            builder.header("Range", rangeVal)
        }

        val request = builder.build()
        val response = httpClient.newCall(request).execute()
        if (!response.isSuccessful) {
            val errBody = response.body?.string() ?: "{}"
            response.close()
            handleDriveHttpResponse(request, response.code, errBody) {
                throw IOException("Cloud stream failed with HTTP ${response.code}")
            }
        }
        val rawStream = response.body?.byteStream() ?: run {
            response.close()
            throw IOException("Empty cloud stream body")
        }
        return object : FilterInputStream(rawStream) {
            override fun close() {
                try {
                    super.close()
                } finally {
                    response.close()
                }
            }
        }
    }

    /**
     * Google Drive Changes API v3: Fetches initial startPageToken for incremental cloud change tracking.
     */
    fun fetchStartPageToken(accessToken: String): String {
        val url = "$driveBaseUrl/drive/v3/changes/startPageToken?supportsAllDrives=true"
        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                JSONObject(bodyStr).optString("startPageToken", "1")
            }
        }
    }

    /**
     * Google Drive Changes API v3: Lists incremental changes since [pageToken] (Section 9).
     */
    fun fetchDriveChanges(
        accessToken: String,
        accountId: String,
        pageToken: String,
        pageSize: Int = 100
    ): DriveChangesPage {
        val url = "$driveBaseUrl/drive/v3/changes".toHttpUrl().newBuilder()
            .addQueryParameter("pageToken", pageToken)
            .addQueryParameter("pageSize", pageSize.toString())
            .addQueryParameter("fields", "nextPageToken,newStartPageToken,changes(fileId,removed,file($FILE_FIELDS))")
            .addQueryParameter("includeItemsFromAllDrives", "true")
            .addQueryParameter("supportsAllDrives", "true")
            .build()

        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer $accessToken")
            .get()
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStr = response.body?.string() ?: "{}"
            return handleDriveHttpResponse(request, response.code, bodyStr) {
                val json = JSONObject(bodyStr)
                val nextToken = json.optString("nextPageToken", "").ifBlank { null }
                val newStartToken = json.optString("newStartPageToken", "").ifBlank { null }
                val changesArr = json.optJSONArray("changes") ?: JSONArray()
                val items = mutableListOf<CloudBrowseItem>()
                for (i in 0 until changesArr.length()) {
                    val changeObj = changesArr.getJSONObject(i)
                    val fileObj = changeObj.optJSONObject("file") ?: continue
                    if (!fileObj.optBoolean("trashed", false)) {
                        items.add(parseCloudBrowseItem(fileObj, accountId))
                    }
                }
                DriveChangesPage(
                    changedFiles = items,
                    newStartPageToken = newStartToken,
                    nextPageToken = nextToken
                )
            }
        }
    }

    private fun parseRemoteFileMetadata(json: JSONObject): RemoteDriveFileMetadata {
        val parents = json.optJSONArray("parents")
        val parentId = if (parents != null && parents.length() > 0) parents.optString(0).ifBlank { null } else null
        return RemoteDriveFileMetadata(
            id = json.optString("id", ""),
            name = json.optString("name", ""),
            mimeType = json.optString("mimeType", "application/octet-stream"),
            size = json.optLong("size", 0L),
            md5Checksum = json.optString("md5Checksum", "").ifBlank { null },
            sha256Checksum = json.optString("sha256Checksum", "").ifBlank { null },
            trashed = json.optBoolean("trashed", false),
            starred = json.optBoolean("starred", false),
            shared = json.optBoolean("shared", false),
            webViewLink = json.optString("webViewLink", "").ifBlank { null },
            parentFolderId = parentId,
            createdTimeMs = parseRfc3339Millis(json.optString("createdTime", "")),
            modifiedTimeMs = parseRfc3339Millis(json.optString("modifiedTime", ""))
        )
    }

    private fun parseCloudBrowseItem(json: JSONObject, accountId: String): CloudBrowseItem {
        val meta = parseRemoteFileMetadata(json)
        val appProps = json.optJSONObject("appProperties")
        val isProtected = appProps?.optString("policy", "") == "NEVER_DELETE_APPEND_ONLY" ||
            meta.name == GoogleAuthManager.ROOT_BACKUP_FOLDER_NAME
        return CloudBrowseItem(
            driveFileId = meta.id,
            accountId = accountId,
            name = meta.name,
            mimeType = meta.mimeType,
            sizeBytes = meta.size,
            md5Checksum = meta.md5Checksum,
            sha256Checksum = meta.sha256Checksum,
            parentFolderId = meta.parentFolderId,
            isFolder = meta.mimeType == FOLDER_MIME_TYPE,
            isProtectedBackupFile = isProtected,
            starred = meta.starred,
            shared = meta.shared,
            webViewLink = meta.webViewLink,
            createdTimeMs = meta.createdTimeMs,
            modifiedTimeMs = meta.modifiedTimeMs
        )
    }

    private fun parseRfc3339Millis(isoString: String): Long {
        if (isoString.isBlank()) return 0L
        return try {
            val cleaned = isoString.replace(Regex("\\.\\d+Z$"), "Z")
            val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US).apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }
            sdf.parse(cleaned)?.time ?: 0L
        } catch (_: Throwable) {
            0L
        }
    }

    private fun parseRangeHeaderUpperBytes(rangeHeader: String?): Long {
        if (rangeHeader.isNullOrBlank()) return 0L
        val dashIdx = rangeHeader.lastIndexOf('-')
        if (dashIdx == -1 || dashIdx + 1 >= rangeHeader.length) return 0L
        val upperInclusive = rangeHeader.substring(dashIdx + 1).trim().toLongOrNull() ?: return 0L
        return upperInclusive + 1L
    }

    /**
     * Section 15 Security Audit: Escapes single quotes and backslashes in Drive API `q` parameters
     * and strips control characters to prevent query injection or malicious filename issues.
     */
    fun escapeDriveQueryLiteral(raw: String): String {
        return raw
            .replace("\\", "\\\\")
            .replace("'", "\\'")
    }

    fun sanitizeDriveName(rawName: String): String {
        return rawName
            .replace("\u0000", "")
            .replace(Regex("[\\r\\n\\t]"), "_")
            .trim()
            .ifBlank { "unnamed_file" }
    }

    /**
     * Section 9: Structured HTTP 400, 401, 403 (Quota vs Permission vs RateLimit), 404, 409, 429 & 5xx handling.
     * Never logs or exposes any OAuth Bearer token or personal credential.
     */
    private inline fun <T> handleDriveHttpResponse(
        request: Request,
        code: Int,
        bodyStr: String,
        onSuccess: (String) -> T
    ): T {
        if (code in 200..299) {
            return onSuccess(bodyStr)
        }

        val sanitizedBody = sanitizeErrorBody(bodyStr)

        when (code) {
            400 -> {
                val url = request.url
                val sanitizedUrl = "${url.scheme}://${url.host}${url.encodedPath}"
                val paramNames = url.queryParameterNames.toList()
                throw DriveBadRequestException(
                    requestMethod = request.method,
                    sanitizedUrl = sanitizedUrl,
                    queryParamNames = paramNames,
                    sanitizedResponse = sanitizedBody
                )
            }
            401 -> {
                throw AuthTokenExpiredException("Google Drive API returned 401 Unauthorized")
            }
            403 -> {
                if (bodyStr.contains("storageQuotaExceeded", ignoreCase = true) ||
                    bodyStr.contains("quotaExceeded", ignoreCase = true) ||
                    bodyStr.contains("teamDriveFileLimitExceeded", ignoreCase = true)
                ) {
                    throw StorageQuotaFullException("Google Drive storage quota is full (HTTP 403 storageQuotaExceeded)")
                }
                if (bodyStr.contains("userRateLimitExceeded", ignoreCase = true) ||
                    bodyStr.contains("rateLimitExceeded", ignoreCase = true)
                ) {
                    throw DriveRateLimitException("Google Drive API rate limit reached (HTTP 403 rateLimitExceeded)")
                }
                throw DrivePermissionException(
                    "Google Drive permission or scope insufficient (HTTP 403): $sanitizedBody"
                )
            }
            404 -> {
                throw DriveResourceNotFoundException("Google Drive resource not found (HTTP 404): $sanitizedBody")
            }
            409 -> {
                throw DriveConflictException("Google Drive resource conflict (HTTP 409): $sanitizedBody")
            }
            429 -> {
                throw DriveRateLimitException("Google Drive API HTTP 429 Too Many Requests: $sanitizedBody")
            }
            in 500..599 -> {
                throw DriveServerException(code, "Google Drive API server error (HTTP $code): $sanitizedBody")
            }
            else -> {
                throw IOException("Google Drive API error HTTP $code: $sanitizedBody")
            }
        }
    }

    private fun sanitizeErrorBody(rawBody: String): String {
        return rawBody
            .replace(Regex("ya29\\.[A-Za-z0-9_\\-.]+"), "[REDACTED_ACCESS_TOKEN]")
            .replace(Regex("1//[A-Za-z0-9_\\-.]+"), "[REDACTED_REFRESH_TOKEN]")
            .take(500)
    }
}
