package com.permanentbackup.gdrive.domain.engine

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.os.Environment
import android.webkit.MimeTypeMap
import androidx.core.content.ContextCompat
import com.permanentbackup.gdrive.domain.model.DiscoveredSourceFile
import com.permanentbackup.gdrive.domain.model.FolderAccessStatus
import com.permanentbackup.gdrive.domain.model.FolderCoverageItem
import com.permanentbackup.gdrive.domain.model.LocalBrowseItem
import java.io.File
import java.io.InputStream

interface StorageFileSource {
    val hasStoragePermission: Boolean
    fun scanFilesSequence(computeHashes: Boolean = true): Sequence<DiscoveredSourceFile>
    fun getFolderCoverageReport(backedUpCountsByCategory: Map<String, Int>): List<FolderCoverageItem>
    fun browseLocalDirectory(relativeSubPath: String = ""): List<LocalBrowseItem>
    fun openInputStream(sourceUri: String): InputStream
    fun exists(sourceUri: String): Boolean
}

/**
 * Section 5B: Local Phone Explorer & Storage Scanner.
 * Discovers accessible files across Shared Internal Storage, DCIM, Downloads, Documents, Pictures,
 * Videos, Audio, WhatsApp Media, Telegram Downloads, and removable SD Card storage while strictly
 * refusing to read Android OS restricted app-private directories (Android/data, Android/obb, /data/data).
 */
class AndroidStorageScanner(
    private val context: Context?,
    private val customRootDirectories: List<File>? = null,
    private val permissionOverride: Boolean? = null
) : StorageFileSource {

    companion object {
        val TARGET_STANDARD_FOLDERS = listOf(
            "DCIM" to "DCIM",
            "Pictures" to "Pictures",
            "Movies" to "Videos",
            "Video" to "Videos",
            "Music" to "Audio",
            "Download" to "Downloads",
            "Documents" to "Documents",
            "Android/media/com.whatsapp/WhatsApp/Media" to "WhatsApp Media",
            "WhatsApp/Media" to "WhatsApp Media",
            "Download/Telegram" to "Telegram Downloads",
            "Telegram" to "Telegram Downloads",
            "Android/media/org.telegram.messenger/Telegram" to "Telegram Downloads"
        )

        val ANDROID_RESTRICTED_PATHS = listOf(
            "Android/data" to "Restricted by Android 11+ Scoped Storage (App-private external data cannot be read by backup apps)",
            "Android/obb" to "Restricted by Android 11+ Scoped Storage (Opaque binary expansion files)",
            "/data/data" to "Restricted by Android Linux Kernel Sandbox (Internal private app databases & keys)"
        )
    }

    override val hasStoragePermission: Boolean
        get() {
            permissionOverride?.let { return it }
            val ctx = context ?: return true
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                if (Environment.isExternalStorageManager()) return true
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                val hasImages = ContextCompat.checkSelfPermission(
                    ctx,
                    Manifest.permission.READ_MEDIA_IMAGES
                ) == PackageManager.PERMISSION_GRANTED
                val hasVideo = ContextCompat.checkSelfPermission(
                    ctx,
                    Manifest.permission.READ_MEDIA_VIDEO
                ) == PackageManager.PERMISSION_GRANTED
                val hasAudio = ContextCompat.checkSelfPermission(
                    ctx,
                    Manifest.permission.READ_MEDIA_AUDIO
                ) == PackageManager.PERMISSION_GRANTED
                if (hasImages || hasVideo || hasAudio) return true
            }
            return ContextCompat.checkSelfPermission(
                ctx,
                Manifest.permission.READ_EXTERNAL_STORAGE
            ) == PackageManager.PERMISSION_GRANTED
        }

    fun resolveStorageRoots(): List<File> {
        if (customRootDirectories != null) return customRootDirectories
        val roots = mutableListOf<File>()
        try {
            val primaryRoot = Environment.getExternalStorageDirectory()
            if (primaryRoot != null && primaryRoot.exists()) {
                roots.add(primaryRoot)
            }
        } catch (_: Throwable) {
        }
        // Discover removable SD Card roots if present
        context?.let { ctx ->
            try {
                val externalDirs = ContextCompat.getExternalFilesDirs(ctx, null)
                for (dir in externalDirs) {
                    if (dir == null) continue
                    val path = dir.absolutePath
                    val androidIdx = path.indexOf("/Android/data")
                    if (androidIdx > 0) {
                        val sdRoot = File(path.substring(0, androidIdx))
                        if (sdRoot.exists() && sdRoot.canRead() && roots.none { it.absolutePath == sdRoot.absolutePath }) {
                            roots.add(sdRoot)
                        }
                    }
                }
            } catch (_: Throwable) {
            }
        }
        return roots
    }

    override fun scanFilesSequence(computeHashes: Boolean): Sequence<DiscoveredSourceFile> = sequence {
        if (!hasStoragePermission) return@sequence
        val roots = resolveStorageRoots()

        for ((rootIndex, rootDir) in roots.withIndex()) {
            val rootPrefix = if (rootIndex == 0) "" else "SDCard_${rootIndex}/"
            val rootPath = rootDir.canonicalPath

            rootDir.walkTopDown()
                .onEnter { dir ->
                    val rel = dir.canonicalPath.removePrefix(rootPath).trimStart('/', '\\').replace('\\', '/')
                    !isRestrictedAndroidPath(rel) &&
                        !dir.name.startsWith(".thumbnails") &&
                        dir.canRead()
                }
                .filter { it.isFile && it.canRead() }
                .forEach { file ->
                    val relPath = file.canonicalPath.removePrefix(rootPath).trimStart('/', '\\')
                        .replace('\\', '/')
                    val fullRelativePath = "$rootPrefix$relPath"
                    val category = classifyFolderCategory(relPath, rootIndex > 0)
                    val hashes = if (computeHashes && file.length() <= 64L * 1024 * 1024) {
                        try {
                            SourceFileReadOnlyGuard.computeStreamingHashes {
                                SourceFileReadOnlyGuard.openReadOnlyFileStream(file)
                            }
                        } catch (_: Throwable) {
                            null
                        }
                    } else {
                        null
                    }

                    yield(
                        DiscoveredSourceFile(
                            sourceUri = file.absolutePath,
                            sourceFileIdentity = "vol$rootIndex:$relPath",
                            relativePath = fullRelativePath,
                            displayName = file.name,
                            folderCategory = category,
                            fileSize = file.length(),
                            lastModifiedMs = file.lastModified(),
                            mimeType = guessMimeType(file.name),
                            sha256Hash = hashes?.sha256Hex,
                            md5Hash = hashes?.md5Hex
                        )
                    )
                }
        }
    }

    /**
     * Section 5B: Interactive Local Phone Storage Explorer.
     * Lists folders and files in [relativeSubPath] while preventing path traversal and
     * never attempting to read restricted Android/data or Android/obb directories.
     */
    override fun browseLocalDirectory(relativeSubPath: String): List<LocalBrowseItem> {
        if (!hasStoragePermission) return emptyList()
        val roots = resolveStorageRoots()
        val primaryRoot = roots.firstOrNull() ?: return emptyList()

        val cleanSub = relativeSubPath
            .replace('\\', '/')
            .split('/')
            .filter { it.isNotBlank() && it != "." && it != ".." }
            .joinToString("/")

        if (isRestrictedAndroidPath(cleanSub)) {
            return emptyList()
        }

        val targetDir = if (cleanSub.isBlank()) primaryRoot else File(primaryRoot, cleanSub)
        if (!targetDir.exists() || !targetDir.isDirectory || !targetDir.canRead()) {
            return emptyList()
        }

        val rootCanonical = primaryRoot.canonicalPath
        val targetCanonical = targetDir.canonicalPath
        if (!targetCanonical.startsWith(rootCanonical)) {
            return emptyList()
        }

        val children = targetDir.listFiles() ?: return emptyList()
        return children
            .sortedWith(compareByDescending<File> { it.isDirectory }.thenBy { it.name.lowercase() })
            .map { child ->
                val rel = child.canonicalPath.removePrefix(rootCanonical).trimStart('/', '\\').replace('\\', '/')
                val restricted = isRestrictedAndroidPath(rel)
                val childCount = if (child.isDirectory && !restricted && child.canRead()) {
                    child.list()?.size ?: 0
                } else {
                    0
                }
                LocalBrowseItem(
                    absolutePath = child.absolutePath,
                    relativePath = rel,
                    name = child.name,
                    isDirectory = child.isDirectory,
                    sizeBytes = if (child.isFile) child.length() else 0L,
                    lastModifiedMs = child.lastModified(),
                    mimeType = if (child.isDirectory) "inode/directory" else guessMimeType(child.name),
                    folderCategory = classifyFolderCategory(rel, false),
                    isRestrictedByOs = restricted,
                    backupEnabled = !restricted,
                    childCount = childCount
                )
            }
    }

    override fun getFolderCoverageReport(backedUpCountsByCategory: Map<String, Int>): List<FolderCoverageItem> {
        val items = mutableListOf<FolderCoverageItem>()
        val roots = resolveStorageRoots()
        val primaryRoot = roots.firstOrNull()
        val nowMs = System.currentTimeMillis()

        val seenCategories = mutableSetOf<String>()
        for ((subPath, category) in TARGET_STANDARD_FOLDERS) {
            if (!seenCategories.add(category)) continue
            val folderFile = primaryRoot?.let { File(it, subPath) }
            val status = when {
                !hasStoragePermission -> FolderAccessStatus.PERMISSION_REQUIRED
                folderFile != null && folderFile.exists() && folderFile.canRead() -> FolderAccessStatus.ACCESSIBLE
                else -> FolderAccessStatus.NOT_PRESENT_ON_DEVICE
            }
            val discoveredCount = if (status == FolderAccessStatus.ACCESSIBLE && folderFile != null) {
                folderFile.walkTopDown().count { it.isFile && it.canRead() }
            } else {
                0
            }
            items.add(
                FolderCoverageItem(
                    folderName = category,
                    relativePath = subPath,
                    accessStatus = status,
                    discoveredFileCount = discoveredCount,
                    backedUpFileCount = backedUpCountsByCategory[category] ?: 0,
                    restrictionExplanation = when (status) {
                        FolderAccessStatus.ACCESSIBLE -> "Full Read-Only Backup Active -> Android_Permanent_Backup/$subPath"
                        FolderAccessStatus.PERMISSION_REQUIRED -> "Requires User Storage Permission (MANAGE_EXTERNAL_STORAGE / Media Access)"
                        FolderAccessStatus.NOT_PRESENT_ON_DEVICE -> "Folder not yet created on device (Will be auto-detected when created)"
                        FolderAccessStatus.RESTRICTED_BY_ANDROID_OS -> "Restricted by Android OS"
                    },
                    backupEnabled = status == FolderAccessStatus.ACCESSIBLE,
                    lastScanTimeMs = nowMs
                )
            )
        }

        // Dynamically include any newly created top-level folders on primary storage
        if (hasStoragePermission && primaryRoot != null && primaryRoot.exists()) {
            primaryRoot.listFiles()
                ?.filter { it.isDirectory && it.canRead() && !it.name.startsWith(".") && it.name != "Android" }
                ?.forEach { topDir ->
                    val alreadyListed = TARGET_STANDARD_FOLDERS.any {
                        it.first.equals(topDir.name, ignoreCase = true)
                    }
                    if (!alreadyListed) {
                        val count = topDir.walkTopDown().count { it.isFile && it.canRead() }
                        items.add(
                            FolderCoverageItem(
                                folderName = "Dynamic Folder: ${topDir.name}",
                                relativePath = topDir.name,
                                accessStatus = FolderAccessStatus.ACCESSIBLE,
                                discoveredFileCount = count,
                                backedUpFileCount = backedUpCountsByCategory[topDir.name] ?: 0,
                                restrictionExplanation = "Dynamically discovered folder -> Android_Permanent_Backup/${topDir.name}",
                                backupEnabled = true,
                                lastScanTimeMs = nowMs
                            )
                        )
                    }
                }
        }

        // Include external SD card roots if present
        if (roots.size > 1) {
            for (sdIdx in 1 until roots.size) {
                val sdRoot = roots[sdIdx]
                val count = if (hasStoragePermission && sdRoot.canRead()) {
                    sdRoot.walkTopDown().count { it.isFile && it.canRead() }
                } else {
                    0
                }
                items.add(
                    FolderCoverageItem(
                        folderName = "External SD Card #$sdIdx",
                        relativePath = "SDCard_$sdIdx",
                        accessStatus = if (hasStoragePermission) FolderAccessStatus.ACCESSIBLE else FolderAccessStatus.PERMISSION_REQUIRED,
                        discoveredFileCount = count,
                        backedUpFileCount = backedUpCountsByCategory["SD Card"] ?: 0,
                        restrictionExplanation = "External SD Card Storage -> Android_Permanent_Backup/SDCard_$sdIdx",
                        backupEnabled = true,
                        lastScanTimeMs = nowMs
                    )
                )
            }
        }

        // Explicitly append Restricted Android OS Folders so user knows they are never falsely claimed as backed up
        for ((restrictedPath, explanation) in ANDROID_RESTRICTED_PATHS) {
            items.add(
                FolderCoverageItem(
                    folderName = "Restricted: $restrictedPath",
                    relativePath = restrictedPath,
                    accessStatus = FolderAccessStatus.RESTRICTED_BY_ANDROID_OS,
                    discoveredFileCount = 0,
                    backedUpFileCount = 0,
                    restrictionExplanation = explanation,
                    backupEnabled = false,
                    lastScanTimeMs = nowMs
                )
            )
        }

        return items
    }

    override fun openInputStream(sourceUri: String): InputStream {
        return SourceFileReadOnlyGuard.openReadOnlyFileStream(File(sourceUri))
    }

    override fun exists(sourceUri: String): Boolean {
        return File(sourceUri).exists()
    }

    private fun isRestrictedAndroidPath(relativePath: String): Boolean {
        val norm = relativePath.replace('\\', '/').trimStart('/')
        return norm.equals("Android/data", ignoreCase = true) ||
            norm.startsWith("Android/data/", ignoreCase = true) ||
            norm.equals("Android/obb", ignoreCase = true) ||
            norm.startsWith("Android/obb/", ignoreCase = true) ||
            norm.startsWith("data/data", ignoreCase = true)
    }

    private fun classifyFolderCategory(relativePath: String, isSdCard: Boolean): String {
        if (isSdCard) return "SD Card"
        val normalized = relativePath.replace('\\', '/')
        return when {
            normalized.startsWith("DCIM", ignoreCase = true) -> "DCIM"
            normalized.startsWith("Pictures", ignoreCase = true) -> "Pictures"
            normalized.startsWith("Movies", ignoreCase = true) || normalized.startsWith("Video", ignoreCase = true) -> "Videos"
            normalized.startsWith("Music", ignoreCase = true) -> "Audio"
            normalized.contains("WhatsApp/Media", ignoreCase = true) -> "WhatsApp Media"
            normalized.contains("Telegram", ignoreCase = true) -> "Telegram Downloads"
            normalized.startsWith("Download", ignoreCase = true) -> "Downloads"
            normalized.startsWith("Documents", ignoreCase = true) -> "Documents"
            else -> normalized.substringBefore('/', "Other Folders")
        }
    }

    private fun guessMimeType(fileName: String): String {
        val ext = fileName.substringAfterLast('.', "").lowercase()
        if (ext.isBlank()) return "application/octet-stream"
        val mapMime = try {
            MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext)
        } catch (_: Throwable) {
            null
        }
        return mapMime ?: when (ext) {
            "jpg", "jpeg" -> "image/jpeg"
            "png" -> "image/png"
            "webp" -> "image/webp"
            "mp4" -> "video/mp4"
            "mkv" -> "video/x-matroska"
            "mp3" -> "audio/mpeg"
            "pdf" -> "application/pdf"
            "txt", "log", "md" -> "text/plain"
            "json" -> "application/json"
            "zip" -> "application/zip"
            else -> "application/octet-stream"
        }
    }
}
