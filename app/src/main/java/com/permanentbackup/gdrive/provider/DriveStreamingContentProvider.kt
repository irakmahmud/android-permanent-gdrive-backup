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
    }

    override fun onCreate(): Boolean = true

    override fun getType(uri: Uri): String {
        return uri.getQueryParameter("mime") ?: "application/octet-stream"
    }

    override fun query(
        uri: Uri,
        projection: Array<out String>?,
        selection: String?,
        selectionArgs: Array<out String>?,
        sortOrder: String?
    ): Cursor {
        val displayName = uri.getQueryParameter("name") ?: uri.lastPathSegment ?: "cloud_stream_file"
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
        val driveFileId = uri.lastPathSegment
            ?: throw FileNotFoundException("Missing Google Drive file ID in stream URI: $uri")
        val app = context?.applicationContext as? PermanentBackupApp
            ?: throw FileNotFoundException("Application context unavailable for streaming")

        val pipe = ParcelFileDescriptor.createPipe()
        val readSide = pipe[0]
        val writeSide = pipe[1]

        thread(name = "DriveCloudStream-$driveFileId", isDaemon = true) {
            ParcelFileDescriptor.AutoCloseOutputStream(writeSide).use { outStream ->
                try {
                    val token = app.repository.authManager.getValidAccessToken()
                    app.repository.driveApiClient.openCloudStream(
                        accessToken = token,
                        driveFileId = driveFileId
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
