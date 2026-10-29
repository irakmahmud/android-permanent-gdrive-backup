package com.permanentbackup.gdrive.provider

import android.content.ContentProvider
import android.content.ContentValues
import android.database.Cursor
import android.database.MatrixCursor
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.provider.OpenableColumns
import com.permanentbackup.gdrive.PermanentBackupApp
import com.permanentbackup.gdrive.data.remote.DriveApiClient
import java.io.FileNotFoundException
import kotlin.concurrent.thread

/**
 * Section 5D: Android ContentProvider enabling Cloud Streaming of Google Drive files
 * (`content://com.permanentbackup.gdrive.stream/files/<driveFileId>`) via OS pipe streaming
 * with a bounded 64 KiB buffer — without downloading the entire file to permanent local storage first.
 *
 * Note: This uses Android's standard ContentProvider pipe streaming architecture rather than a
 * desktop kernel FUSE virtual drive mount (which unrooted Android OS does not permit).
 */
class DriveStreamingContentProvider : ContentProvider() {

    companion object {
        const val AUTHORITY = "com.permanentbackup.gdrive.stream"

        fun buildStreamUri(driveFileId: String, displayName: String, mimeType: String, sizeBytes: Long): Uri {
            return Uri.Builder()
                .scheme("content")
                .authority(AUTHORITY)
                .appendPath("files")
                .appendPath(driveFileId)
                .appendQueryParameter("name", displayName)
                .appendQueryParameter("mime", mimeType)
                .appendQueryParameter("size", sizeBytes.toString())
                .build()
        }

        fun buildOfflineFileUri(localFileName: String, displayName: String, mimeType: String, sizeBytes: Long): Uri {
            return Uri.Builder()
                .scheme("content")
                .authority(AUTHORITY)
                .appendPath("offline")
                .appendPath(localFileName)
                .appendQueryParameter("name", displayName)
                .appendQueryParameter("mime", mimeType)
                .appendQueryParameter("size", sizeBytes.toString())
                .build()
        }

        fun resolveExportedMimeType(rawMime: String): String {
            if (!rawMime.startsWith("application/vnd.google-apps.") || rawMime == "application/vnd.google-apps.folder") {
                return rawMime
            }
            return when (rawMime) {
                "application/vnd.google-apps.document" ->
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                "application/vnd.google-apps.spreadsheet" ->
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                "application/vnd.google-apps.presentation" ->
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                "application/vnd.google-apps.script" ->
                    "application/vnd.google-apps.script+json"
                else -> "application/pdf"
            }
        }

        fun resolveExportedDisplayName(displayName: String, rawMime: String): String {
            if (!rawMime.startsWith("application/vnd.google-apps.") || rawMime == "application/vnd.google-apps.folder") {
                return displayName
            }
            val ext = when (rawMime) {
                "application/vnd.google-apps.document" -> ".docx"
                "application/vnd.google-apps.spreadsheet" -> ".xlsx"
                "application/vnd.google-apps.presentation" -> ".pptx"
                "application/vnd.google-apps.script" -> ".json"
                else -> ".pdf"
            }
            return if (displayName.endsWith(ext, ignoreCase = true)) displayName else "$displayName$ext"
        }
    }

    override fun onCreate(): Boolean = true

    override fun getType(uri: Uri): String {
        val rawMime = uri.getQueryParameter("mime") ?: "application/octet-stream"
        return resolveExportedMimeType(rawMime)
    }

    override fun query(
        uri: Uri,
        projection: Array<out String>?,
        selection: String?,
        selectionArgs: Array<out String>?,
        sortOrder: String?
    ): Cursor {
        val rawName = uri.getQueryParameter("name") ?: uri.lastPathSegment ?: "cloud_stream_file"
        val rawMime = uri.getQueryParameter("mime") ?: "application/octet-stream"
        val displayName = resolveExportedDisplayName(rawName, rawMime)
        val sizeBytes = uri.getQueryParameter("size")?.toLongOrNull() ?: -1L
        val cols = projection ?: arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE)
        val cursor = MatrixCursor(cols, 1)
        val row = cols.map { col ->
            when (col) {
                OpenableColumns.DISPLAY_NAME -> displayName
                OpenableColumns.SIZE -> if (sizeBytes >= 0L) sizeBytes else null
                else -> null
            }
        }.toTypedArray()
        cursor.addRow(row)
        return cursor
    }

    override fun openFile(uri: Uri, mode: String): ParcelFileDescriptor {
        if (mode != "r") {
            throw SecurityException("DriveStreamingContentProvider is strictly read-only (mode '$mode' forbidden)")
        }
        val pathSegments = uri.pathSegments
        val isOfflineRoute = pathSegments.firstOrNull() == "offline"
        val targetId = uri.lastPathSegment
            ?: throw FileNotFoundException("Missing file identifier in URI: $uri")

        val app = context?.applicationContext as? PermanentBackupApp

        // 1. Check DB-recorded offlineLocalPath first for seekable ParcelFileDescriptor
        if (app != null) {
            val dbPinnedPath = kotlinx.coroutines.runBlocking {
                app.database.backupDao().getCloudFileById(targetId)?.offlineLocalPath
            }
            if (!dbPinnedPath.isNullOrBlank()) {
                val dbFile = java.io.File(dbPinnedPath)
                if (dbFile.exists() && dbFile.isFile) {
                    return ParcelFileDescriptor.open(dbFile, ParcelFileDescriptor.MODE_READ_ONLY)
                }
            }
        }

        // 2. Fallback check in cacheDir and filesDir offline_pinned_drive_cache
        val candidateDirs = listOfNotNull(
            context?.cacheDir?.resolve("offline_pinned_drive_cache"),
            context?.filesDir?.resolve("offline_pinned_drive_cache")
        )
        for (cacheDir in candidateDirs) {
            if (cacheDir.exists()) {
                val found = cacheDir.walkTopDown().firstOrNull { f ->
                    f.isFile && (if (isOfflineRoute) f.name == targetId else f.name.startsWith("${targetId}_"))
                }
                if (found != null) {
                    return ParcelFileDescriptor.open(found, ParcelFileDescriptor.MODE_READ_ONLY)
                }
            }
        }

        if (app == null) {
            throw FileNotFoundException("Application context unavailable for streaming")
        }

        val pipe = ParcelFileDescriptor.createPipe()
        val readSide = pipe[0]
        val writeSide = pipe[1]
        val rawMime = uri.getQueryParameter("mime")

        thread(name = "DriveCloudStream-$targetId", isDaemon = true) {
            ParcelFileDescriptor.AutoCloseOutputStream(writeSide).use { outStream ->
                try {
                    val token = app.repository.authManager.getValidAccessToken()
                    app.repository.driveApiClient.openCloudStream(
                        accessToken = token,
                        driveFileId = targetId,
                        workspaceMimeType = rawMime
                    ).use { cloudIn ->
                        val buffer = ByteArray(DriveApiClient.STREAM_BUFFER_SIZE_BYTES)
                        while (true) {
                            val read = cloudIn.read(buffer)
                            if (read == -1) break
                            outStream.write(buffer, 0, read)
                        }
                        outStream.flush()
                    }
                } catch (_: Throwable) {
                    // Reader sees EOF/broken pipe cleanly if network drops mid-stream
                }
            }
        }

        return readSide
    }

    override fun insert(uri: Uri, values: ContentValues?): Uri? {
        throw UnsupportedOperationException("Insert is forbidden on read-only DriveStreamingContentProvider")
    }

    override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?): Int {
        throw UnsupportedOperationException("Delete is strictly forbidden by Never-Delete policy")
    }

    override fun update(
        uri: Uri,
        values: ContentValues?,
        selection: String?,
        selectionArgs: Array<out String>?
    ): Int {
        throw UnsupportedOperationException("Update is forbidden on read-only DriveStreamingContentProvider")
    }
}
