package com.permanentbackup.gdrive.data.remote

import com.permanentbackup.gdrive.domain.model.NeverDeletePolicyViolationException
import okhttp3.Interceptor
import okhttp3.Response
import okio.Buffer

/**
 * Architectural Transport-Layer Guard enforcing Section 6: NEVER DELETE & NEVER OVERWRITE.
 * Every outgoing HTTP call to Google Drive API passes through this interceptor.
 * Any attempt to call DELETE, trash a file, or overwrite an existing Drive file ID is blocked immediately.
 */
class NeverDeleteGuardInterceptor : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val method = request.method.uppercase()
        val urlString = request.url.toString()
        val path = request.url.encodedPath

        // Rule 6: Never call HTTP DELETE under any circumstance
        if (method == "DELETE") {
            throw NeverDeletePolicyViolationException(
                "CRITICAL POLICY VIOLATION: HTTP DELETE is strictly forbidden by Never-Delete Backup Policy ($urlString)"
            )
        }

        // Rule 7: Never call trash, emptyTrash, or destructive endpoints
        val lowerPath = path.lowercase()
        if (lowerPath.contains("trash") || lowerPath.contains("delete") || lowerPath.contains("prune")) {
            throw NeverDeletePolicyViolationException(
                "CRITICAL POLICY VIOLATION: Trash/Delete endpoint is strictly forbidden ($urlString)"
            )
        }

        // Rule 8: Never PATCH or PUT to an existing file resource to overwrite or trash it.
        // Only PUT requests to an active Resumable Upload session (containing upload_id or uploadType=resumable) are allowed.
        if (method == "PATCH") {
            throw NeverDeletePolicyViolationException(
                "CRITICAL POLICY VIOLATION: HTTP PATCH (modifying/trashing existing Drive files) is forbidden ($urlString)"
            )
        }

        if (method == "PUT") {
            val hasUploadId = request.url.queryParameter("upload_id") != null
            val isResumableUpload = request.url.queryParameter("uploadType") == "resumable"
            if (!hasUploadId && !isResumableUpload) {
                throw NeverDeletePolicyViolationException(
                    "CRITICAL POLICY VIOLATION: Direct HTTP PUT overwrite without active resumable upload session is forbidden ($urlString)"
                )
            }
        }

        // Inspect small JSON metadata requests (like POST) to ensure "trashed": true is never sent
        val body = request.body
        if (body != null && body.contentLength() in 1..4096) {
            val contentType = body.contentType()?.subtype?.lowercase() ?: ""
            if (contentType.contains("json")) {
                val buffer = Buffer()
                body.writeTo(buffer)
                val bodyText = buffer.readUtf8()
                if (Regex("\"trashed\"\\s*:\\s*true", RegexOption.IGNORE_CASE).containsMatchIn(bodyText)) {
                    throw NeverDeletePolicyViolationException(
                        "CRITICAL POLICY VIOLATION: Setting trashed=true in Drive metadata is strictly forbidden"
                    )
                }
            }
        }

        return chain.proceed(request)
    }
}
