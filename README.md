# Android Permanent Google Drive Backup App

A strictly **Upload-Only, Append-Only, Never-Delete** Android backup application engineered in **Kotlin**, **Jetpack Compose (Material 3)**, **Room**, **WorkManager**, **Foreground Service**, and **Google Drive REST API v3 (Streaming Resumable Uploads)** with **100% Remote Cloud APK Building** via **CircleCI** (`.circleci/config.yml`) and **GitHub Cloud Runners**.

---

## 1. Core Architectural Guarantees

### A. Strictly Upload-Only & Never-Delete (`NeverDeleteGuardInterceptor`)
1. **Phone File Deleted**: Deleting a file on the phone only marks `localExists = false` in local Room metadata. The cloud backup in `"Android_Permanent_Backup"` on Google Drive is **never** deleted.
2. **Phone Folder Deleted**: Deleting a folder on the phone leaves the corresponding folder and all backed-up files on Google Drive completely intact.
3. **Phone File Renamed / Moved**: The previous backup on Google Drive remains untouched; the renamed/moved file is uploaded as a new entry preserving its folder path.
4. **Phone File Modified**: When an existing file changes (size, modification timestamp, or SHA-256 hash), Version $N$ on Google Drive is preserved intact and Version $N+1$ (`filename__v2_<hash8>.ext`) is uploaded as a separate Drive file.
5. **Transport-Level Enforcement**: `NeverDeleteGuardInterceptor` inspects every outgoing HTTP request to `googleapis.com` and throws `NeverDeletePolicyViolationException` if any code path ever attempts `HTTP DELETE`, `HTTP PATCH`, direct `PUT` overwrite without a resumable `upload_id`, or `"trashed": true`.

### B. Streaming Resumable Upload & Zero Local Temp Copy
- Uses Google Drive API v3 Resumable Upload (`POST /upload/drive/v3/files?uploadType=resumable`).
- Streams chunks (multiples of `256 KiB`) directly from a read-only `FileInputStream` using a fixed **`64 KiB` RAM buffer**.
- **0 bytes** of temporary file copies are created on the phone's internal storage—enabling 1 GB, 5 GB, or 10 GB+ file backups even when the phone has less than 1 GB of free space.
- Persists `uploadedBytes` and `resumableSessionUri` in Room after every chunk. On network failure or app restart, queries `Content-Range: bytes */total` (`HTTP 308 Resume Incomplete`) and resumes from the exact confirmed byte offset.

---

## 2. Realistic Backup Limitations & Android OS Restrictions Report (Deliverable 15)

In full transparency and compliance with Android security architecture and Google Drive API limits:

1. **Android OS Restricted App-Private Folders**:
   - `/data/data/<package>` (Internal sandboxed app directories)
   - `/storage/emulated/0/Android/data/<package>` (Blocked by Android 11+ Scoped Storage even with `MANAGE_EXTERNAL_STORAGE`)
   - `/storage/emulated/0/Android/obb/<package>` (Opaque expansion files restricted by Android 11+)
   - **Behavior**: These directories cannot be read on unrooted Android 11–16 devices. The app explicitly lists them as `RESTRICTED_BY_ANDROID_OS` in the **Folder Coverage** screen and **never** claims they are backed up.
2. **Files Deleted Before Initial Upload Finishes**:
   - If a newly created file on the phone is deleted by the user *before* its first upload to Google Drive completes, that file cannot be recovered. The engine records `FailureReason.SOURCE_DELETED_BEFORE_UPLOAD` in the **Failed Uploads** screen.
3. **Android Background Scheduling & Android 15/16 Quotas**:
   - Android OS controls `WorkManager` execution windows, Doze Mode maintenance windows, Battery Saver restrictions, and Android 15/16 `dataSync` Foreground Service 6-hour timeout quotas. The app handles `Service.onTimeout(startId, fgsType)` by saving chunk progress to Room and re-enqueueing continuation via `WorkManager`, rather than promising exact-second background execution.
4. **Google OAuth 2.0 Client Configuration**:
   - The app defaults to the least-privilege `https://www.googleapis.com/auth/drive.file` scope (only accessing the `"Android_Permanent_Backup"` folder tree created by the app).
   - For Google Sign-In (`GoogleSignInOptions`) in production, the user's Google Cloud Console project must register the APK's package name (`com.permanentbackup.gdrive` / `com.permanentbackup.gdrive.debug`) and SHA-1 signing certificate fingerprint, or the user can supply an authorized OAuth 2.0 access/refresh token in the **Google Account** screen.
5. **Google Drive Checksum Availability**:
   - After every upload, `DriveApiClient.verifyRemoteFile` checks remote `size`, `trashed == false`, and compares `sha256Checksum` / `md5Checksum` (`CHECKSUM_AND_SIZE_VERIFIED`). If Google Drive omits checksum fields for a particular MIME type, the app verifies exact byte size and metadata and records `SIZE_AND_METADATA_VERIFIED_NO_REMOTE_CHECKSUM`.
