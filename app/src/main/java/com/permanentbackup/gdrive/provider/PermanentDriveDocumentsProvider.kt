package com.permanentbackup.gdrive.provider

import android.database.Cursor
import android.database.MatrixCursor
import android.os.CancellationSignal
import android.os.ParcelFileDescriptor
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import kotlinx.coroutines.runBlocking
import java.io.File
import java.io.FileNotFoundException
import kotlin.concurrent.thread

/**
 * Android SAF DocumentsProvider ("Permanent Drive" system-wide virtual cloud provider).
 *
 * Technical Architecture & Android OS Boundaries vs. Windows `G:\` Virtual Drive:
 * - On Windows Desktop, Google Drive uses a kernel/filesystem driver (Dokan/ProjFS) to mount a POSIX/Win32
 *   drive letter (`G:\`). Unrooted Android does not allow user-space apps to mount kernel FUSE filesystems
 *   globally across all legacy apps using `java.io.File("/mnt/...")`.
 * - Instead, Android's official system-wide virtual drive contract is `android.provider.DocumentsProvider`
 *   (Storage Access Framework). Any SAF-compliant app (Files by Google, Office suites, PDF viewers, media players)
 *   can browse "Permanent Drive" directly from the system file picker.
 * - Seekability nuance: `ParcelFileDescriptor.createPipe()` is non-seekable (FIFO stream), which works for sequential
 *   readers; for random-access/seekable readers (e.g. video/PDF players), files pinned offline in
 *   `offline_pinned_drive_cache` are served via a real seekable file descriptor (`ParcelFileDescriptor.open(file, MODE_READ_ONLY)`).
 * - Strict Zero-Deletion Invariant: `COLUMN_FLAGS` never advertises `FLAG_SUPPORTS_DELETE`, `FLAG_SUPPORTS_WRITE`,
 *   `FLAG_SUPPORTS_REMOVE`, or `FLAG_SUPPORTS_MOVE`, and all mutating overrides (`deleteDocument`, `removeDocument`,
 *   `createDocument`, `renameDocument`) throw `UnsupportedOperationException` so third-party apps can NEVER
 *   delete or overwrite cloud backups.
 */
class PermanentDriveDocumentsProvider : DocumentsProvider() {

    companion object {
        const val AUTHORITY = "com.permanentbackup.gdrive.documents"
        const val ROOT_ID = "permanent_drive_root"
        const val DOC_ID_ROOT = "folder:root"
        const val DOC_ID_PINNED_OFFLINE = "folder:pinned_offline"
        const val DOC_ID_VERIFIED_BACKUPS = "folder:verified_backups"

        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            Root.COLUMN_ROOT_ID,
            Root.COLUMN_FLAGS,
            Root.COLUMN_ICON,
            Root.COLUMN_TITLE,
            Root.COLUMN_SUMMARY,
            Root.COLUMN_DOCUMENT_ID,
            Root.COLUMN_MIME_TYPES
        )

        private val DEFAULT_DOCUMENT_PROJECTION = arrayOf(
            Document.COLUMN_DOCUMENT_ID,
            Document.COLUMN_MIME_TYPE,
            Document.COLUMN_DISPLAY_NAME,
            Document.COLUMN_LAST_MODIFIED,
            Document.COLUMN_FLAGS,
            Document.COLUMN_SIZE
        )
    }

    override fun onCreate(): Boolean = true

    override fun queryRoots(projection: Array<out String>?): Cursor {
        val result = MatrixCursor(projection ?: DEFAULT_ROOT_PROJECTION)
        val app = context?.applicationContext as? PermanentBackupApp
        val accountEmail = app?.repository?.authManager?.accountState?.value?.email?.ifBlank { null }
            ?: "Zero-Delete Cloud Vault & Offline Pinned Cache"

        result.newRow().apply {
            add(Root.COLUMN_ROOT_ID, ROOT_ID)
            // Read-only + supports recents & search; NEVER advertises FLAG_SUPPORTS_CREATE or delete capabilities
            add(Root.COLUMN_FLAGS, Root.FLAG_SUPPORTS_IS_CHILD or Root.FLAG_SUPPORTS_SEARCH)
            add(Root.COLUMN_ICON, android.R.drawable.ic_menu_upload)
            add(Root.COLUMN_TITLE, "Permanent Drive")
            add(Root.COLUMN_SUMMARY, accountEmail)
            add(Root.COLUMN_DOCUMENT_ID, DOC_ID_ROOT)
            add(Root.COLUMN_MIME_TYPES, "*/*")
        }
        return result
    }

    override fun queryDocument(documentId: String?, projection: Array<out String>?): Cursor {
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        val docId = documentId ?: DOC_ID_ROOT
        val app = context?.applicationContext as? PermanentBackupApp

        when (docId) {
            DOC_ID_ROOT -> {
                includeDirectoryRow(result, DOC_ID_ROOT, "Permanent Drive", System.currentTimeMillis())
            }
            DOC_ID_PINNED_OFFLINE -> {
                includeDirectoryRow(result, DOC_ID_PINNED_OFFLINE, "Pinned Offline (Seekable Local Cache)", System.currentTimeMillis())
            }
            DOC_ID_VERIFIED_BACKUPS -> {
                includeDirectoryRow(result, DOC_ID_VERIFIED_BACKUPS, "Verified Cloud Backups (Read-Only)", System.currentTimeMillis())
            }
            else -> {
                if (app != null) {
                    runBlocking {
                        val cloudId = docId.removePrefix("cloud:")
                        val cached = app.database.backupDao().getCloudFileById(cloudId)
                        if (cached != null) {
                            val mime = if (cached.isFolder) Document.MIME_TYPE_DIR else cached.mimeType
                            includeFileRow(
                                cursor = result,
                                documentId = "cloud:${cached.driveFileId}",
                                displayName = cached.name,
                                mimeType = mime,
                                sizeBytes = cached.sizeBytes,
                                lastModifiedMs = cached.modifiedTimeMs
                            )
                        }
                    }
                }
            }
        }
        return result
    }

    override fun queryChildDocuments(
        parentDocumentId: String?,
        projection: Array<out String>?,
        sortOrder: String?
    ): Cursor {
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        val parentId = parentDocumentId ?: DOC_ID_ROOT
        val app = context?.applicationContext as? PermanentBackupApp ?: return result

        when (parentId) {
            DOC_ID_ROOT -> {
                includeDirectoryRow(result, DOC_ID_PINNED_OFFLINE, "Pinned Offline (Seekable Local Cache)", System.currentTimeMillis())
                includeDirectoryRow(result, DOC_ID_VERIFIED_BACKUPS, "Verified Cloud Backups (Read-Only)", System.currentTimeMillis())
                runBlocking {
                    val rootFiles = app.database.backupDao().getCloudFilesByParent("root")
                    for (item in rootFiles) {
                        includeFileRow(
                            cursor = result,
                            documentId = if (item.isFolder) "folder:${item.driveFileId}" else "cloud:${item.driveFileId}",
                            displayName = item.name,
                            mimeType = if (item.isFolder) Document.MIME_TYPE_DIR else item.mimeType,
                            sizeBytes = item.sizeBytes,
                            lastModifiedMs = item.modifiedTimeMs
                        )
                    }
                }
            }
            DOC_ID_PINNED_OFFLINE -> {
                val cacheDir = File(context?.filesDir, "offline_pinned_drive_cache")
                if (cacheDir.exists() && cacheDir.isDirectory) {
                    cacheDir.listFiles()?.forEach { file ->
                        if (file.isFile) {
                            includeFileRow(
                                cursor = result,
                                documentId = "local_pinned:${file.name}",
                                displayName = file.name.substringAfter("_", file.name),
                                mimeType = resolveMimeFromExtension(file.name),
                                sizeBytes = file.length(),
                                lastModifiedMs = file.lastModified()
                            )
                        }
                    }
                }
            }
            DOC_ID_VERIFIED_BACKUPS -> {
                runBlocking {
                    val verified = app.database.backupDao().getVerifiedBackupsPaged(limit = 200, offset = 0)
                    for (record in verified) {
                        val driveId = record.driveFileId ?: continue
                        includeFileRow(
                            cursor = result,
                            documentId = "cloud:$driveId",
                            displayName = "${record.displayName} (v${record.backupVersion})",
                            mimeType = record.mimeType,
                            sizeBytes = record.fileSize,
                            lastModifiedMs = record.lastModifiedMs
                        )
                    }
                }
            }
            else -> {
                val folderDriveId = parentId.removePrefix("folder:").removePrefix("cloud:")
                runBlocking {
                    val children = app.database.backupDao().getCloudFilesByParent(folderDriveId)
                    for (item in children) {
                        includeFileRow(
                            cursor = result,
                            documentId = if (item.isFolder) "folder:${item.driveFileId}" else "cloud:${item.driveFileId}",
                            displayName = item.name,
                            mimeType = if (item.isFolder) Document.MIME_TYPE_DIR else item.mimeType,
                            sizeBytes = item.sizeBytes,
                            lastModifiedMs = item.modifiedTimeMs
                        )
                    }
                }
            }
        }
        return result
    }

    override fun querySearchDocuments(
        rootId: String?,
        query: String?,
        projection: Array<out String>?
    ): Cursor {
        val result = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        val searchQuery = query?.trim().orEmpty()
        if (searchQuery.isEmpty()) return result
        val app = context?.applicationContext as? PermanentBackupApp ?: return result

        runBlocking {
            val matches = app.database.backupDao().searchAllCloudFiles(searchQuery)
            for (item in matches) {
                includeFileRow(
                    cursor = result,
                    documentId = if (item.isFolder) "folder:${item.driveFileId}" else "cloud:${item.driveFileId}",
                    displayName = item.name,
                    mimeType = if (item.isFolder) Document.MIME_TYPE_DIR else item.mimeType,
                    sizeBytes = item.sizeBytes,
                    lastModifiedMs = item.modifiedTimeMs
                )
            }
        }
        return result
    }

    override fun isChildDocument(parentDocumentId: String?, documentId: String?): Boolean {
        if (parentDocumentId == null || documentId == null) return false
        if (parentDocumentId == DOC_ID_ROOT) return true
        return documentId.startsWith("cloud:") || documentId.startsWith("local_pinned:") || documentId.startsWith("folder:")
    }

    override fun openDocument(
        documentId: String?,
        mode: String?,
        signal: CancellationSignal?
    ): ParcelFileDescriptor {
        if (mode != null && mode != "r") {
            throw SecurityException("PermanentDriveDocumentsProvider is strictly read-only (Zero-Deletion invariant)")
        }
        val docId = documentId ?: throw FileNotFoundException("Missing documentId")
        val app = context?.applicationContext as? PermanentBackupApp
            ?: throw FileNotFoundException("Application context unavailable")

        // 1. Direct local pinned file request -> return seekable ParcelFileDescriptor
        if (docId.startsWith("local_pinned:")) {
            val fileName = docId.removePrefix("local_pinned:")
            val cacheDir = File(context?.filesDir, "offline_pinned_drive_cache")
            val target = File(cacheDir, fileName)
            if (target.exists() && target.isFile) {
                return ParcelFileDescriptor.open(target, ParcelFileDescriptor.MODE_READ_ONLY)
            }
            throw FileNotFoundException("Pinned local file not found: $fileName")
        }

        val driveFileId = docId.removePrefix("cloud:")
        val cachedEntity = runBlocking { app.database.backupDao().getCloudFileById(driveFileId) }

        // 2. If cloud file is already pinned offline, return seekable file descriptor for full random-access compatibility
        val localPinnedPath = cachedEntity?.offlineLocalPath
        if (!localPinnedPath.isNullOrBlank()) {
            val localFile = File(localPinnedPath)
            if (localFile.exists() && localFile.isFile) {
                return ParcelFileDescriptor.open(localFile, ParcelFileDescriptor.MODE_READ_ONLY)
            }
        }
        val cacheDir = File(context?.filesDir, "offline_pinned_drive_cache")
        if (cacheDir.exists()) {
            val matchingPinned = cacheDir.listFiles()?.firstOrNull {
                it.isFile && it.name.startsWith("${driveFileId.take(12)}_")
            }
            if (matchingPinned != null) {
                return ParcelFileDescriptor.open(matchingPinned, ParcelFileDescriptor.MODE_READ_ONLY)
            }
        }

        // 3. Otherwise stream on-demand from Google Drive API via OS pipe
        val pipe = ParcelFileDescriptor.createPipe()
        val readSide = pipe[0]
        val writeSide = pipe[1]
        val workspaceMime = cachedEntity?.mimeType

        thread(name = "SAFStream-$driveFileId", isDaemon = true) {
            ParcelFileDescriptor.AutoCloseOutputStream(writeSide).use { outStream ->
                try {
                    val token = app.repository.authManager.getValidAccessToken()
                    app.repository.driveApiClient.openCloudStream(
                        accessToken = token,
                        driveFileId = driveFileId,
                        workspaceMimeType = workspaceMime
                    ).use { cloudIn ->
                        val buffer = ByteArray(DriveApiClient.STREAM_BUFFER_SIZE_BYTES)
                        while (signal?.isCanceled != true) {
                            val read = cloudIn.read(buffer)
                            if (read == -1) break
                            outStream.write(buffer, 0, read)
                        }
                        outStream.flush()
                    }
                } catch (_: Throwable) {
                    // Clean EOF on cancellation or network drop
                }
            }
        }
        return readSide
    }

    override fun deleteDocument(documentId: String?) {
        throw UnsupportedOperationException("Strict Zero-Deletion Policy: Deleting files from Permanent Drive is permanently disabled.")
    }

    override fun removeDocument(documentId: String?, parentDocumentId: String?) {
        throw UnsupportedOperationException("Strict Zero-Deletion Policy: Removing files from Permanent Drive is permanently disabled.")
    }

    override fun createDocument(parentDocumentId: String?, mimeType: String?, displayName: String?): String {
        throw UnsupportedOperationException("PermanentDriveDocumentsProvider is a read-only SAF mount; use Share Sheet or App Upload to add backups.")
    }

    override fun renameDocument(documentId: String?, displayName: String?): String {
        throw UnsupportedOperationException("Renaming cloud backup files via SAF is disabled to preserve immutable backup history.")
    }

    private fun includeDirectoryRow(
        cursor: MatrixCursor,
        documentId: String,
        displayName: String,
        lastModifiedMs: Long
    ) {
        cursor.newRow().apply {
            add(Document.COLUMN_DOCUMENT_ID, documentId)
            add(Document.COLUMN_MIME_TYPE, Document.MIME_TYPE_DIR)
            add(Document.COLUMN_DISPLAY_NAME, displayName)
            add(Document.COLUMN_LAST_MODIFIED, lastModifiedMs)
            add(Document.COLUMN_FLAGS, 0) // Strictly 0 flags: no delete, no write, no rename
            add(Document.COLUMN_SIZE, null)
        }
    }

    private fun includeFileRow(
        cursor: MatrixCursor,
        documentId: String,
        displayName: String,
        mimeType: String,
        sizeBytes: Long,
        lastModifiedMs: Long
    ) {
        val resolvedMime = if (mimeType.startsWith("application/vnd.google-apps.") && mimeType != "application/vnd.google-apps.folder") {
            "application/pdf"
        } else {
            mimeType
        }
        cursor.newRow().apply {
            add(Document.COLUMN_DOCUMENT_ID, documentId)
            add(Document.COLUMN_MIME_TYPE, resolvedMime)
            add(Document.COLUMN_DISPLAY_NAME, displayName)
            add(Document.COLUMN_LAST_MODIFIED, lastModifiedMs)
            add(Document.COLUMN_FLAGS, 0) // Strictly read-only: no delete/write/move flags
            add(Document.COLUMN_SIZE, sizeBytes)
        }
    }

    private fun resolveMimeFromExtension(name: String): String {
        val ext = name.substringAfterLast('.', "").lowercase()
        return when (ext) {
            "pdf" -> "application/pdf"
            "jpg", "jpeg" -> "image/jpeg"
            "png" -> "image/png"
            "mp4" -> "video/mp4"
            "mp3" -> "audio/mpeg"
            "txt", "md", "log" -> "text/plain"
            "docx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            "pptx" -> "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            "zip" -> "application/zip"
            else -> "application/octet-stream"
        }
    }
}
