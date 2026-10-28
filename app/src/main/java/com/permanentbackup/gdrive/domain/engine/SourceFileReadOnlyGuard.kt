package com.permanentbackup.gdrive.domain.engine

import com.permanentbackup.gdrive.domain.model.SourceFileModificationForbiddenException
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.security.MessageDigest

data class StreamHashes(
    val sha256Hex: String,
    val md5Hex: String,
    val totalBytesRead: Long
)

/**
 * Guarantees that local source files are accessed strictly in READ-ONLY mode,
 * streamed with a bounded 64 KiB buffer, and never copied to temp storage or modified.
 */
object SourceFileReadOnlyGuard {
    private const val HASH_BUFFER_SIZE = 64 * 1024 // 64 KiB fixed RAM buffer

    fun openReadOnlyFileStream(file: File): InputStream {
        if (!file.exists() || !file.canRead()) {
            throw IOException("Source file does not exist or is not readable: ${file.absolutePath}")
        }
        return FileInputStream(file)
    }

    /**
     * Skips deterministically to [offsetBytes] on a freshly opened read-only [InputStream]
     * without copying any bytes to temporary disk storage or loading the file into RAM.
     */
    fun skipStreamToOffset(inputStream: InputStream, offsetBytes: Long) {
        var remaining = offsetBytes
        val skipBuffer = ByteArray(8192)
        while (remaining > 0L) {
            val skipped = inputStream.skip(remaining)
            if (skipped > 0L) {
                remaining -= skipped
            } else {
                // Fallback read if underlying stream implementation returns 0 on skip()
                val toRead = minOf(skipBuffer.size.toLong(), remaining).toInt()
                val read = inputStream.read(skipBuffer, 0, toRead)
                if (read == -1) {
                    throw IOException("Reached EOF before skipping to resume offset $offsetBytes")
                }
                remaining -= read
            }
        }
    }

    /**
     * Computes both SHA-256 and MD5 hashes in a single streaming pass using a 64 KiB buffer.
     */
    fun computeStreamingHashes(inputStreamProvider: () -> InputStream): StreamHashes {
        val sha256 = MessageDigest.getInstance("SHA-256")
        val md5 = MessageDigest.getInstance("MD5")
        var totalRead = 0L
        val buffer = ByteArray(HASH_BUFFER_SIZE)

        inputStreamProvider().use { stream ->
            while (true) {
                val read = stream.read(buffer)
                if (read == -1) break
                sha256.update(buffer, 0, read)
                md5.update(buffer, 0, read)
                totalRead += read
            }
        }

        return StreamHashes(
            sha256Hex = sha256.digest().toHexString(),
            md5Hex = md5.digest().toHexString(),
            totalBytesRead = totalRead
        )
    }

    /**
     * Explicit guard forbidding any write, rename, or delete operation on user source files.
     */
    fun forbidSourceFileMutation(operationName: String, sourcePath: String): Nothing {
        throw SourceFileModificationForbiddenException(
            "READ-ONLY POLICY VIOLATION: Operation '$operationName' on source file '$sourcePath' is strictly prohibited."
        )
    }

    private fun ByteArray.toHexString(): String {
        val hexChars = "0123456789abcdef"
        val result = StringBuilder(size * 2)
        for (byte in this) {
            val v = byte.toInt() and 0xFF
            result.append(hexChars[v ushr 4])
            result.append(hexChars[v and 0x0F])
        }
        return result.toString()
    }
}
