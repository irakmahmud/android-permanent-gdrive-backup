package com.permanentbackup.gdrive.data.remote

import com.permanentbackup.gdrive.domain.model.AuthTokenExpiredException
import com.permanentbackup.gdrive.domain.model.DriveQuotaInfo
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
import java.io.IOException
import java.io.InputStream
import java.util.concurrent.TimeUnit

data class RemoteDriveFileMetadata(
    val id: String,
    val name: String,
    val size: Long,
    val md5Checksum: String?,
    val sha256Checksum: String?,
    val trashed: Boolean
)

data class ChunkUploadResult(
    val completed: Boolean,
    val confirmedUploadedBytes: Long,
    val driveFileId: String?,
    val sessionUri: String
)

/**
 * Google Drive API v3 Client implementing:
 * - Root folder creation ("Android_Permanent_Backup") & nested subfolder hierarchy preservation
 * - Resumable Upload session creation & expired session detection (404/410)
 * - Streaming chunk upload with bounded 64 KiB RAM buffer (Zero temp file copy, Zero full-file RAM load)
 * - Remote Metadata, File Size & Checksum verification
 * - Strictly zero destructive methods (enforced both by API design and NeverDeleteGuardInterceptor)
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
            handleAuthAndQuotaErrors(response.code, response.body?.string() ?: "{}") { bodyStr ->
                val json = JSONObject(bodyStr)
                val quota = json.optJSONObject("storageQuota") ?: JSONObject()
                return DriveQuotaInfo(
                    limitBytes = quota.optLong("limit", 15L * 1024 * 1024 * 1024),
                    usageBytes = quota.optLong("usage", 0L),
                    usageInDriveBytes = quota.optLong("usageInDrive", 0L),
                    usageInTrashBytes = quota.optLong("usageInDriveTrash", 0L)
                )
            }
        }
    }

    /**
     * Finds or creates the dedicated root folder "Android_Permanent_Backup" on Google Drive.
     */
    fun ensureFolderExists(
        accessToken: String,
        folderName: String,
        parentFolderId: String? = null
    ): String {
        val escapedName = folderName.replace("'", "\\'")
        val parentClause = if (parentFolderId != null) " and '$parentFolderId' in parents" else ""
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
            handleAuthAndQuotaErrors(response.code, bodyStr) {
                val files = JSONObject(bodyStr).optJSONArray("files")
                if (files != null && files.length() > 0) {
                    val existingId = files.getJSONObject(0).optString("id", "")
                    if (existingId.isNotBlank()) return existingId
                }
            }
        }

        // Create folder if not found
        val metadataJson = JSONObject().apply {
            put("name", folderName)
            put("mimeType", FOLDER_MIME_TYPE)
            if (parentFolderId != null) {
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
            return handleAuthAndQuotaErrors(response.code, bodyStr) {
                val id = JSONObject(bodyStr).optString("id", "")
                if (id.isBlank()) throw IOException("Drive folder creation returned empty folder ID")
                id
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
        val metadataJson = JSONObject().apply {
            put("name", driveFileName)
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
            return handleAuthAndQuotaErrors(response.code, bodyStr) {
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
                    handleAuthAndQuotaErrors(code, bodyStr) {
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
                    handleAuthAndQuotaErrors(code, bodyStr) {
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
            return handleAuthAndQuotaErrors(response.code, bodyStr) {
                val json = JSONObject(bodyStr)
                val id = json.optString("id", "")
                val trashed = json.optBoolean("trashed", false)
                val remoteSize = json.optLong("size", -1L)
                val remoteMd5 = json.optString("md5Checksum", "").ifBlank { null }
                val remoteSha256 = json.optString("sha256Checksum", "").ifBlank { null }

                if (id != driveFileId || trashed || remoteSize != expectedSize) {
                    return@handleAuthAndQuotaErrors VerificationStatus.VERIFICATION_FAILED
                }

                // Verify SHA-256 or MD5 when provided by Google Drive
                if (remoteSha256 != null && expectedSha256 != null) {
                    return@handleAuthAndQuotaErrors if (remoteSha256.equals(expectedSha256, ignoreCase = true)) {
                        VerificationStatus.CHECKSUM_AND_SIZE_VERIFIED
                    } else {
                        VerificationStatus.VERIFICATION_FAILED
                    }
                }

                if (remoteMd5 != null && expectedMd5 != null) {
                    return@handleAuthAndQuotaErrors if (remoteMd5.equals(expectedMd5, ignoreCase = true)) {
                        VerificationStatus.CHECKSUM_AND_SIZE_VERIFIED
                    } else {
                        VerificationStatus.VERIFICATION_FAILED
                    }
                }

                // Fallback when Google Drive does not compute checksums for specific MIME types
                VerificationStatus.SIZE_AND_METADATA_VERIFIED_NO_REMOTE_CHECKSUM
            }
        }
    }

    private fun parseRangeHeaderUpperBytes(rangeHeader: String?): Long {
        if (rangeHeader.isNullOrBlank()) return 0L
        // Format: "bytes=0-262143" -> confirmed bytes = 262143 + 1 = 262144
        val dashIdx = rangeHeader.lastIndexOf('-')
        if (dashIdx == -1 || dashIdx + 1 >= rangeHeader.length) return 0L
        val upperInclusive = rangeHeader.substring(dashIdx + 1).trim().toLongOrNull() ?: return 0L
        return upperInclusive + 1L
    }

    private inline fun <T> handleAuthAndQuotaErrors(
        code: Int,
        bodyStr: String,
        onSuccess: (String) -> T
    ): T {
        if (code in 200..299) {
            return onSuccess(bodyStr)
        }
        if (code == 401) {
            throw AuthTokenExpiredException("Google Drive API returned 401 Unauthorized")
        }
        if (code == 403 && (bodyStr.contains("storageQuotaExceeded", ignoreCase = true) ||
                bodyStr.contains("quotaExceeded", ignoreCase = true))
        ) {
            throw StorageQuotaFullException("Google Drive storage quota is full (HTTP 403 storageQuotaExceeded)")
        }
        throw IOException("Google Drive API error HTTP $code: $bodyStr")
    }
}
