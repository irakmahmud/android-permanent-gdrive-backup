package com.permanentbackup.gdrive.domain.engine

import android.content.Context
import android.os.Environment
import com.permanentbackup.gdrive.data.local.BackupDao
import com.permanentbackup.gdrive.data.local.CloudFileEntity
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import com.permanentbackup.gdrive.data.remote.GoogleAuthManager
import com.permanentbackup.gdrive.domain.model.CloudBrowseItem
import com.permanentbackup.gdrive.domain.model.RestoreOutcome
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream

/**
 * Section 5D, 5E, 5F, 10 & 15:
 * Manages Cloud File Restore (strictly non-overwriting & path-traversal safe),
 * Offline File Pinning within a bounded low-storage Cache Budget,
 * and Cloud Streaming Preview without full-file download.
 */
class CloudFileOperationsManager(
    private val context: Context? = null,
    private val backupDao: BackupDao,
    private val driveApiClient: DriveApiClient,
    private val authManager: GoogleAuthManager,
    private val customRestoreRootDir: File? = null,
    private val customOfflineCacheDir: File? = null
) {
    companion object {
        const val PREVIEW_MAX_BYTES = 16 * 1024L // 16 KiB streaming preview snippet
    }

    /**
     * Section 15 Security Audit: Sanitizes any cloud or local filename against path traversal
     * (`../`, `/`, `\`, null bytes, leading dots) so restored/offline files never escape target dir.
     */
    fun sanitizeSafeLocalFileName(rawFileName: String): String {
        val stripped = rawFileName
            .replace("\u0000", "")
            .replace("\\", "_")
            .replace("/", "_")
            .replace("..", "_")
            .replace(Regex("[\\r\\n\\t:*?\"<>|]"), "_")
            .trim()
            .trimStart('.')
        return stripped.ifBlank { "restored_file" }
    }

    /**
     * Section 5F: Restores a file or specific backup version from Google Drive to the phone.
     * NEVER overwrites an existing local file! If a file with the same name already exists in the
     * target restore directory, generates a unique non-conflicting filename automatically.
     */
    suspend fun restoreCloudFileToLocal(
        driveFileId: String,
        originalFileName: String,
        versionNumber: Int = 1,
        targetSubfolder: String = ""
    ): RestoreOutcome {
        return try {
            val accessToken = authManager.getValidAccessToken()
            val baseRestoreDir = resolveRestoreBaseDirectory()
            val safeSub = targetSubfolder
                .replace('\\', '/')
                .split('/')
                .map { sanitizeSafeLocalFileName(it) }
                .filter { it.isNotBlank() && it != "restored_file" }
                .joinToString(File.separator)

            val destDir = if (safeSub.isNotBlank()) File(baseRestoreDir, safeSub) else baseRestoreDir
            if (!destDir.exists()) {
                destDir.mkdirs()
            }

            // Ensure canonical path stays inside baseRestoreDir (Path Traversal Guard)
            val canonicalBase = baseRestoreDir.canonicalPath
            val canonicalDestDir = destDir.canonicalPath
            if (!canonicalDestDir.startsWith(canonicalBase)) {
                return RestoreOutcome(
                    success = false,
                    restoredLocalPath = null,
                    restoredBytes = 0L,
                    message = "Security block: Invalid target restore path traversal rejected."
                )
            }

            val safeName = sanitizeSafeLocalFileName(originalFileName)
            var candidateFile = File(destDir, safeName)
            var avoidedOverwrite = false

            if (candidateFile.exists()) {
                avoidedOverwrite = true
                val dotIdx = safeName.lastIndexOf('.')
                val ts = System.currentTimeMillis()
                val uniqueName = if (dotIdx > 0) {
                    val base = safeName.substring(0, dotIdx)
                    val ext = safeName.substring(dotIdx)
                    "${base}_restored_v${versionNumber}_${ts}${ext}"
                } else {
                    "${safeName}_restored_v${versionNumber}_${ts}"
                }
                candidateFile = File(destDir, uniqueName)
            }

            val bytesWritten = FileOutputStream(candidateFile).use { fos ->
                driveApiClient.downloadFileStreaming(
                    accessToken = accessToken,
                    driveFileId = driveFileId,
                    outputStream = fos
                )
            }

            RestoreOutcome(
                success = true,
                restoredLocalPath = candidateFile.absolutePath,
                restoredBytes = bytesWritten,
                message = if (avoidedOverwrite) {
                    "Restored safely as '${candidateFile.name}' (Existing local file preserved untouched)"
                } else {
                    "Restored to '${candidateFile.absolutePath}'"
                },
                avoidedOverwrite = avoidedOverwrite
            )
        } catch (e: Exception) {
            RestoreOutcome(
                success = false,
                restoredLocalPath = null,
                restoredBytes = 0L,
                message = "Restore failed: ${e.message}"
            )
        }
    }

    /**
     * Section 5E & Section 10: Pins a Google Drive file for Offline Access while enforcing
     * the user's configured low-storage Offline Cache Budget.
     */
    suspend fun pinCloudFileOffline(
        item: CloudBrowseItem,
        maxBudgetBytes: Long
    ): RestoreOutcome {
        val currentUsed = backupDao.sumOfflineCachedBytes()
        if (currentUsed + item.sizeBytes > maxBudgetBytes) {
            return RestoreOutcome(
                success = false,
                restoredLocalPath = null,
                restoredBytes = 0L,
                message = "Offline cache budget exceeded (${currentUsed + item.sizeBytes} bytes > $maxBudgetBytes budget). Increase budget in Settings or unpin unused offline files."
            )
        }

        return try {
            val accessToken = authManager.getValidAccessToken()
            val cacheRoot = resolveOfflineCacheDirectory()
            val accountDir = File(cacheRoot, sanitizeSafeLocalFileName(item.accountId.ifBlank { "default" }))
            if (!accountDir.exists()) accountDir.mkdirs()

            val safeName = sanitizeSafeLocalFileName("${item.driveFileId}_${item.name}")
            val offlineFile = File(accountDir, safeName)

            val written = FileOutputStream(offlineFile).use { fos ->
                driveApiClient.downloadFileStreaming(
                    accessToken = accessToken,
                    driveFileId = item.driveFileId,
                    outputStream = fos
                )
            }

            backupDao.upsertCloudFile(
                CloudFileEntity(
                    driveFileId = item.driveFileId,
                    accountId = item.accountId,
                    name = item.name,
                    mimeType = item.mimeType,
                    sizeBytes = item.sizeBytes,
                    md5Checksum = item.md5Checksum,
                    sha256Checksum = item.sha256Checksum,
                    parentFolderId = item.parentFolderId,
                    isFolder = item.isFolder,
                    isProtectedBackupFile = item.isProtectedBackupFile,
                    starred = item.starred,
                    shared = item.shared,
                    webViewLink = item.webViewLink,
                    createdTimeMs = item.createdTimeMs,
                    modifiedTimeMs = item.modifiedTimeMs,
                    isPinnedOffline = true,
                    offlineLocalPath = offlineFile.absolutePath,
                    offlineCachedBytes = written
                )
            )

            RestoreOutcome(
                success = true,
                restoredLocalPath = offlineFile.absolutePath,
                restoredBytes = written,
                message = "Pinned '${item.name}' for offline access ($written bytes)"
            )
        } catch (e: Exception) {
            RestoreOutcome(
                success = false,
                restoredLocalPath = null,
                restoredBytes = 0L,
                message = "Failed to pin offline: ${e.message}"
            )
        }
    }

    /**
     * Unpins an offline cached file to free local phone storage (never affects the cloud file on Google Drive).
     */
    suspend fun unpinCloudFileOffline(driveFileId: String) {
        val existing = backupDao.getCloudFileById(driveFileId)
        existing?.offlineLocalPath?.let { path ->
            try {
                val f = File(path)
                if (f.exists() && f.canonicalPath.startsWith(resolveOfflineCacheDirectory().canonicalPath)) {
                    f.delete()
                }
            } catch (_: Throwable) {
            }
        }
        backupDao.updateCloudFileOfflinePin(
            driveFileId = driveFileId,
            pinned = false,
            localPath = null,
            cachedBytes = 0L
        )
    }

    /**
     * Section 5D: Streams the first 16 KiB of a cloud file for instant preview without full download.
     */
    fun streamPreviewSnippet(driveFileId: String, maxBytes: Long = PREVIEW_MAX_BYTES): String {
        return try {
            val token = authManager.getValidAccessToken()
            driveApiClient.openCloudStream(
                accessToken = token,
                driveFileId = driveFileId,
                rangeStartByte = 0L,
                rangeEndByteInclusive = maxBytes - 1L
            ).use { stream ->
                val out = ByteArrayOutputStream()
                val buf = ByteArray(4096)
                var remaining = maxBytes.toInt()
                while (remaining > 0) {
                    val read = stream.read(buf, 0, minOf(buf.size, remaining))
                    if (read == -1) break
                    out.write(buf, 0, read)
                    remaining -= read
                }
                out.toString(Charsets.UTF_8.name())
            }
        } catch (e: Exception) {
            "Preview stream unavailable: ${e.message}"
        }
    }

    private fun resolveRestoreBaseDirectory(): File {
        if (customRestoreRootDir != null) return customRestoreRootDir
        return try {
            val downloads = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
            File(downloads, "Restored_From_Drive")
        } catch (_: Throwable) {
            File(context?.filesDir ?: File("/tmp"), "Restored_From_Drive")
        }
    }

    private fun resolveOfflineCacheDirectory(): File {
        if (customOfflineCacheDir != null) return customOfflineCacheDir
        return File(context?.cacheDir ?: File("/tmp"), "offline_pinned_drive_cache")
    }
}
