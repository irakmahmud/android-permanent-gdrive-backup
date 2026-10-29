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
     * Supports both binary files and Google Workspace documents (Docs/Sheets/Slides exported to Office/PDF).
     * NEVER overwrites an existing local file! If a file with the same name already exists in the
     * target restore directory, generates a unique non-conflicting filename automatically.
     */
    suspend fun restoreCloudFileToLocal(
        driveFileId: String,
        originalFileName: String,
        versionNumber: Int = 1,
        targetSubfolder: String = "",
        mimeType: String? = null
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

            val cachedCloud = backupDao.getCloudFileById(driveFileId)
            val effectiveMime = mimeType ?: cachedCloud?.mimeType ?: "application/octet-stream"
            val isWorkspaceDoc = driveApiClient.isGoogleWorkspaceDocument(effectiveMime)
            val exportFormat = if (isWorkspaceDoc) driveApiClient.resolveWorkspaceExportFormat(effectiveMime) else null

            val baseSafeName = sanitizeSafeLocalFileName(originalFileName)
            val safeName = if (exportFormat != null && !baseSafeName.endsWith(exportFormat.second, ignoreCase = true)) {
                "$baseSafeName${exportFormat.second}"
            } else {
                baseSafeName
            }

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
                if (isWorkspaceDoc && exportFormat != null) {
                    driveApiClient.exportWorkspaceFileStreaming(
                        accessToken = accessToken,
                        driveFileId = driveFileId,
                        exportMimeType = exportFormat.first,
                        outputStream = fos
                    )
                } else {
                    driveApiClient.downloadFileStreaming(
                        accessToken = accessToken,
                        driveFileId = driveFileId,
                        outputStream = fos
                    )
                }
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
     * Recursively downloads/restores an entire Google Drive folder and its nested subfolders
     * to local phone storage without overwriting any existing local file.
     */
    suspend fun restoreCloudFolderRecursively(
        folderItem: CloudBrowseItem,
        targetSubfolder: String = "",
        maxDepth: Int = 6
    ): RestoreOutcome {
        if (!folderItem.isFolder) {
            return restoreCloudFileToLocal(
                driveFileId = folderItem.driveFileId,
                originalFileName = folderItem.name,
                targetSubfolder = targetSubfolder,
                mimeType = folderItem.mimeType
            )
        }
        return try {
            val accessToken = authManager.getValidAccessToken()
            val accountId = folderItem.accountId.ifBlank { "default_account" }
            val rootFolderSafeName = sanitizeSafeLocalFileName(folderItem.name)
            val baseSubfolder = if (targetSubfolder.isNotBlank()) {
                "$targetSubfolder/$rootFolderSafeName"
            } else {
                rootFolderSafeName
            }

            var filesRestored = 0
            var totalBytesRestored = 0L
            var anyAvoidedOverwrite = false

            suspend fun traverseFolder(currentFolderId: String, currentRelSubfolder: String, depth: Int) {
                if (depth > maxDepth) return
                var pageToken: String? = null
                do {
                    val page = driveApiClient.listDriveFiles(
                        accessToken = accessToken,
                        accountId = accountId,
                        parentFolderId = currentFolderId,
                        pageSize = 100,
                        pageToken = pageToken
                    )
                    for (child in page.files) {
                        if (child.isFolder) {
                            val childFolderName = sanitizeSafeLocalFileName(child.name)
                            traverseFolder(
                                currentFolderId = child.driveFileId,
                                currentRelSubfolder = "$currentRelSubfolder/$childFolderName",
                                depth = depth + 1
                            )
                        } else {
                            val outcome = restoreCloudFileToLocal(
                                driveFileId = child.driveFileId,
                                originalFileName = child.name,
                                targetSubfolder = currentRelSubfolder,
                                mimeType = child.mimeType
                            )
                            if (outcome.success) {
                                filesRestored++
                                totalBytesRestored += outcome.restoredBytes
                                if (outcome.avoidedOverwrite) anyAvoidedOverwrite = true
                            }
                        }
                    }
                    pageToken = page.nextPageToken
                } while (!pageToken.isNullOrBlank())
            }

            traverseFolder(folderItem.driveFileId, baseSubfolder, 1)
            val destFolder = File(resolveRestoreBaseDirectory(), baseSubfolder)
            if (!destFolder.exists()) destFolder.mkdirs()

            RestoreOutcome(
                success = true,
                restoredLocalPath = destFolder.absolutePath,
                restoredBytes = totalBytesRestored,
                message = "Restored folder '${folderItem.name}' ($filesRestored files, $totalBytesRestored bytes) to '${destFolder.absolutePath}'",
                avoidedOverwrite = anyAvoidedOverwrite
            )
        } catch (e: Exception) {
            RestoreOutcome(
                success = false,
                restoredLocalPath = null,
                restoredBytes = 0L,
                message = "Folder restore failed: ${e.message}"
            )
        }
    }

    /**
     * One-Way Cloud-to-Local Folder Mirror (Rule 3: Separation of Backup & Mirroring):
     * Downloads a Google Drive folder into `Restored_From_Drive/Drive_Mirror/<folderName>`.
     * - Strictly one-way (Cloud -> Local): NEVER deletes or overwrites anything on Google Drive.
     * - Strictly isolated from backup scanner (`AndroidStorageScanner.isMirrorOrRestoreDirectory()`),
     *   preventing mirrored files from ever looping back into the Permanent Backup upload queue.
     */
    suspend fun mirrorCloudFolderToLocal(folderItem: CloudBrowseItem): RestoreOutcome {
        return restoreCloudFolderRecursively(
            folderItem = folderItem,
            targetSubfolder = "Drive_Mirror"
        )
    }

    /**
     * Section 5E & Section 10: Pins a Google Drive file for Offline Access while enforcing
     * the user's configured low-storage Offline Cache Budget. Supports Google Workspace export.
     */
    suspend fun pinCloudFileOffline(
        item: CloudBrowseItem,
        maxBudgetBytes: Long
    ): RestoreOutcome {
        if (item.isFolder) {
            return pinCloudFolderOfflineRecursively(item, maxBudgetBytes)
        }
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

            val isWorkspaceDoc = driveApiClient.isGoogleWorkspaceDocument(item.mimeType)
            val exportFormat = if (isWorkspaceDoc) driveApiClient.resolveWorkspaceExportFormat(item.mimeType) else null
            val rawPinnedName = if (exportFormat != null && !item.name.endsWith(exportFormat.second, ignoreCase = true)) {
                "${item.driveFileId}_${item.name}${exportFormat.second}"
            } else {
                "${item.driveFileId}_${item.name}"
            }

            val safeName = sanitizeSafeLocalFileName(rawPinnedName)
            val offlineFile = File(accountDir, safeName)

            val written = FileOutputStream(offlineFile).use { fos ->
                if (isWorkspaceDoc && exportFormat != null) {
                    driveApiClient.exportWorkspaceFileStreaming(
                        accessToken = accessToken,
                        driveFileId = item.driveFileId,
                        exportMimeType = exportFormat.first,
                        outputStream = fos
                    )
                } else {
                    driveApiClient.downloadFileStreaming(
                        accessToken = accessToken,
                        driveFileId = item.driveFileId,
                        outputStream = fos
                    )
                }
            }

            if (currentUsed + written > maxBudgetBytes) {
                offlineFile.delete()
                return RestoreOutcome(
                    success = false,
                    restoredLocalPath = null,
                    restoredBytes = 0L,
                    message = "Offline cache budget exceeded after export (${currentUsed + written} bytes > $maxBudgetBytes budget)."
                )
            }

            backupDao.upsertCloudFile(
                CloudFileEntity(
                    driveFileId = item.driveFileId,
                    accountId = item.accountId,
                    name = item.name,
                    mimeType = item.mimeType,
                    sizeBytes = if (item.sizeBytes > 0L) item.sizeBytes else written,
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
     * Recursively pins all files inside a Google Drive folder for offline access within the configured budget.
     */
    suspend fun pinCloudFolderOfflineRecursively(
        folderItem: CloudBrowseItem,
        maxBudgetBytes: Long,
        maxDepth: Int = 5
    ): RestoreOutcome {
        return try {
            val accessToken = authManager.getValidAccessToken()
            val accountId = folderItem.accountId.ifBlank { "default_account" }
            var pinnedFiles = 0
            var totalBytesPinned = 0L

            suspend fun traversePin(currentFolderId: String, depth: Int) {
                if (depth > maxDepth) return
                var pageToken: String? = null
                do {
                    val page = driveApiClient.listDriveFiles(
                        accessToken = accessToken,
                        accountId = accountId,
                        parentFolderId = currentFolderId,
                        pageSize = 100,
                        pageToken = pageToken
                    )
                    for (child in page.files) {
                        if (child.isFolder) {
                            traversePin(child.driveFileId, depth + 1)
                        } else {
                            val outcome = pinCloudFileOffline(child, maxBudgetBytes)
                            if (outcome.success) {
                                pinnedFiles++
                                totalBytesPinned += outcome.restoredBytes
                            }
                        }
                    }
                    pageToken = page.nextPageToken
                } while (!pageToken.isNullOrBlank())
            }

            traversePin(folderItem.driveFileId, 1)
            RestoreOutcome(
                success = true,
                restoredLocalPath = resolveOfflineCacheDirectory().absolutePath,
                restoredBytes = totalBytesPinned,
                message = "Pinned folder '${folderItem.name}' offline ($pinnedFiles files, $totalBytesPinned bytes)"
            )
        } catch (e: Exception) {
            RestoreOutcome(
                success = false,
                restoredLocalPath = null,
                restoredBytes = 0L,
                message = "Folder offline pin failed: ${e.message}"
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
    fun streamPreviewSnippet(driveFileId: String, maxBytes: Long = PREVIEW_MAX_BYTES, workspaceMimeType: String? = null): String {
        return try {
            val token = authManager.getValidAccessToken()
            driveApiClient.openCloudStream(
                accessToken = token,
                driveFileId = driveFileId,
                rangeStartByte = 0L,
                rangeEndByteInclusive = maxBytes - 1L,
                workspaceMimeType = workspaceMimeType
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
