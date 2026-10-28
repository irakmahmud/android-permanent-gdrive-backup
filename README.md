# Permanent Drive v2.0.0 — Android Google Drive Desktop Alternative

A production-grade, strictly **Upload-Only, Append-Only, Never-Delete** Android Google Drive Desktop Alternative engineered in **Kotlin**, **Jetpack Compose (Material 3)**, **Room (WAL + 14 Tables)**, **WorkManager**, **Foreground Service (`dataSync`)**, **Google Identity Services (`AuthorizationClient` + `GoogleAuthUtil`)**, and **Google Drive REST API v3**. Built 100% remotely via **CircleCI** (`.circleci/config.yml`) and **GitHub Cloud Runners** (`.github/workflows/remote-android-build.yml`) with zero local Android SDK/Gradle footprint.

---

## 1. Root-Cause Audit & Permanent Fixes for `v1.0.0` – `v1.3.0`

| # | Historical Issue (`v1.0.0` – `v1.3.0`) | Technical Root Cause | Permanent Fix in `v2.0.0` |
|---|---|---|---|
| 1 | `Google Sign-In failed (ApiException: 10)` | Legacy `GoogleSignIn.getClient` failed with `DEVELOPER_ERROR (10)` because `applicationIdSuffix = ".debug"` changed package name to `com.permanentbackup.gdrive.debug` and the CI debug keystore SHA-1 was unregistered in Google Cloud Console. | Unified package name to `com.permanentbackup.gdrive`, migrated to `Identity.getAuthorizationClient` with `GoogleAuthUtil` fallback, and added a runtime **Signing Certificate SHA-1 / SHA-256 Diagnostic Card** in the `Accounts` screen. |
| 2 | `OAuth 400 Bad Request` | Desktop `rclone` Web/Desktop OAuth client ID + secret was called from Android without matching client type or redirect URI. | Removed all desktop/web OAuth token endpoints (`oauth2.googleapis.com/token` with client secret) and switched to native Android Play Services token issuance. |
| 3 | Browser Redirect Failure | Custom tabs / external browser could not redirect back to a native Android activity without an App Link or custom scheme handler. | Zero external browser flows: authentication uses native Play Services `AuthorizationClient` / `AccountManager` intents inside the app activity. |
| 4 | `127.0.0.1:53682` Localhost Timeout | `rclone authorize "drive"` spins up a desktop loopback web server on `127.0.0.1:53682`, which does not exist on an Android phone. | Completely banned `127.0.0.1:53682` and all loopback OAuth URLs. |
| 5 | `rclone`-Dependent Authentication | Previous versions instructed users to run `rclone` on a PC to extract tokens. | Zero `rclone` dependency anywhere in the Android app. |
| 6 | Manual Token Copy-Paste | Users were asked to manually paste JSON tokens or `ya29.*` access tokens into text fields. | Removed manual token input UI; replaced with one-tap native Android Google Account sign-in. |
| 7 | Post-Account Picker Drive Authorization Failure | `AccountManager.newChooseAccountIntent` only returns account email/type without requesting OAuth scope consent (`drive.file`), causing `UserRecoverableAuthException` on background threads. | `GoogleAuthManager.trySilentOrInteractiveAuthorization` immediately executes `AuthorizationClient.authorize(AuthorizationRequest)` and handles `PendingIntent` consent resolution on the UI thread before saving session state. |
| 8 | Repeated Login Prompts | Access tokens expired after 1 hour (`HTTP 401`) without silent token refresh or persistent `GoogleAccountEntity` tracking. | Added `GoogleAccountEntity` in Room + `EncryptedSharedPreferences`, silent token refresh via `GoogleAuthUtil.clearToken` + `getToken` on `HTTP 401`, and automatic startup session restoration (`syncPersistedAccountOnStartup`). |
| 9 | Refresh Token Exposure Risk | `v1.2.0` and `v1.3.0` injected `DEFAULT_GDRIVE_REFRESH_TOKEN` and `GOOGLE_OAUTH_CLIENT_SECRET` into `BuildConfig.java`. | Removed all secret `buildConfigField` entries from `app/build.gradle.kts`, purged all 5 GitHub repository secrets, deleted compromised `v1.2.0`/`v1.3.0` APK assets, and added an automated CI DEX bytecode secret scanner. |
| 10 | Embedded OAuth Credentials in APK | Anyone decompiling `BuildConfig.class` from `v1.2.0`/`v1.3.0` could extract the embedded refresh token. | `GoogleAuthManager.purgeLegacyCompromisedTokensIfNeeded()` detects any legacy `v1.2.0`/`v1.3.0` token on upgrade, revokes it via `https://oauth2.googleapis.com/revoke`, and clears it from storage. |
| 11 | Inactive CircleCI Pipeline Transparency | CircleCI API returned `{"message":"Project not found"}` because the repo had not been enabled in CircleCI Web UI. | `.circleci/config.yml` is maintained with all 14 stages, while `scripts/fetch_cloud_apk.sh` transparently queries CircleCI API and distinguishes it from GitHub Actions builds. |
| 12 | Confusing GitHub Actions vs CircleCI Builds | Previous reports did not clearly separate CircleCI status from GitHub Actions runner builds. | Build scripts and documentation explicitly distinguish CircleCI API status (`Project not found` until activated with `CIRCLE_TOKEN`) from active GitHub Actions remote cloud builds. |
| 13 | Unverified Physical Device Claims | MockWebServer JVM tests were previously not clearly distinguished from physical Android phone + real Google Drive interactive tests. | All verification reports explicitly separate JVM/MockWebServer automated tests (`24/24 PASSED`) from physical device interactive tests (`Requires Physical Android Device & User Google Account`). |

---

## 2. Core Architecture & 9-Screen Google Drive Desktop Alternative

### A. 3-Layer Immutable Never-Delete Policy
1. **Layer 1 — Domain Engine (`PermanentBackupEngine` & `CloudFileOperationsManager`)**:
   - Local file deletion only sets `localExists = false` in Room (`backup_file_records` and `local_files`). Cloud backups are never deleted.
   - Local file modification preserves Version $N$ on Google Drive and uploads Version $N+1$ (`filename__v2_<hash8>.ext`) as a distinct immutable Drive file (`BackupVersionEntity`).
   - File Restore (`CloudFileOperationsManager.restoreCloudFileToLocal`) never overwrites an existing local file; if `photo.jpg` exists with different content, it restores to `photo (Restored_1).jpg`.
2. **Layer 2 — HTTP Transport Guard (`NeverDeleteGuardInterceptor`)**:
   - Intercepts every outgoing OkHttp request to `googleapis.com` and throws `NeverDeletePolicyViolationException` on any:
     - `HTTP DELETE`
     - `HTTP PATCH`
     - Non-resumable `HTTP PUT` without `uploadType=resumable` and `upload_id=`
     - Request body containing `"trashed"\s*:\s*true`
3. **Layer 3 — Automated Verification (`PermanentBackupEngineTest`)**:
   - 24 JUnit + MockWebServer tests verify every state transition, crash reconciliation, mid-upload pause/resume, per-folder enable/disable filtering, multi-account isolation, legacy token purge + revocation, non-overwriting restore, offline cache eviction budget, and transport-level blocking of `DELETE`/`PATCH`/`PUT`/`trashed:true`.

### B. 12 Mandatory Room Entities + 2 Legacy Compatibility Entities (Database v2, WAL Mode)
- `GoogleAccountEntity` (`google_accounts`)
- `BackupFolderEntity` (`backup_folders`)
- `LocalFileEntity` (`local_files`)
- `CloudFileEntity` (`cloud_files`)
- `BackupVersionEntity` (`backup_versions`)
- `UploadSessionEntity` (`upload_sessions`)
- `SyncJobEntity` (`sync_jobs` — strict state machine: `DISCOVERED -> QUEUED -> PREPARING -> UPLOADING -> VERIFYING -> COMPLETED`, `FAILED_RETRYABLE -> RETRY_SCHEDULED`, `FAILED_ACTION_REQUIRED`)
- `SyncEventEntity` (`sync_events`)
- `FailedOperationEntity` (`failed_operations`)
- `AppSettingsEntity` (`app_settings`)
- `StoragePermissionEntity` (`storage_permissions`)
- `DriveChangeTokenEntity` (`drive_change_tokens`)
- Legacy preserved tables with non-destructive `MIGRATION_1_2`: `BackupFileEntity` (`backup_file_records`) & `DriveFolderCacheEntity` (`drive_folder_cache`).

### C. 9 Material 3 Screens
1. **Home**: Account status, cloud storage quota bar, active transfer speed/ETA banner, quick backup actions, and Never-Delete policy badge.
2. **My Drive**: Full cloud explorer (`FOLDER`, `RECENT`, `STARRED`, `SHARED_WITH_ME`, `SHARED_DRIVES`), breadcrumb navigation, search, sort, list/grid toggle, scope badge (`drive.file` vs `drive.readonly`), 16 KiB range preview, read-only `DriveStreamingContentProvider` streaming, non-overwriting restore, offline pin, and share link.
3. **Phone**: Interactive local storage & SD card explorer with one-tap "Backup File Now" and "Add Folder to Auto-Backup", plus explicit `RESTRICTED_BY_ANDROID_OS` indicators for `/Android/data` and `/Android/obb`.
4. **Backups**: Watched folder toggles, custom SAF folder picker, and per-file immutable version history (`v1`, `v2`, ...).
5. **Recent**: Unified timeline of recent cloud files and audit log events.
6. **Transfers**: Live transfer queue with byte progress, upload speed (`B/s`), ETA countdown, global pause/resume, per-file pause/resume, and failed operation retry queue.
7. **Offline**: Pinned offline files cache with configurable low-storage budget (`maxOfflineCacheMb`) and manual unpin/eviction.
8. **Settings**: Wi-Fi/unmetered toggle, charging toggle, battery saver guard, scheduled interval, offline cache limit, and Drive scope selector (`drive.file` default vs optional `drive.readonly`).
9. **Accounts**: Multi-account switcher, persistent Google Identity Services login, token refresh test, and **OAuth & Signing Certificate SHA-1 / SHA-256 Diagnostic Card**.

---

## 3. Google Cloud Console OAuth 2.0 Setup Guide (Preventing `ApiException: 10`)

Because Google Play Services verifies the **Package Name + APK Signing Certificate SHA-1 Fingerprint** when issuing OAuth 2.0 tokens to an Android app:
1. Open [Google Cloud Console — Credentials](https://console.cloud.google.com/apis/credentials) and enable the **Google Drive API**.
2. Create an **OAuth 2.0 Client ID** of type **Android**:
   - **Package name**: `com.permanentbackup.gdrive`
   - **SHA-1 certificate fingerprint**: Copy the exact SHA-1 fingerprint shown inside the app under **Accounts -> OAuth & Signing Certificate Diagnostics** (or from `apk-signing-certificate.txt` attached to the GitHub Release).
3. Configure the **OAuth Consent Screen** with scopes:
   - `https://www.googleapis.com/auth/drive.file` (Default least-privilege scope)
   - `https://www.googleapis.com/auth/drive.readonly` (Optional if browsing files created outside this app in *My Drive Explorer*).
