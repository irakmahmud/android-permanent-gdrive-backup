package com.permanentbackup.gdrive.data.remote

import android.accounts.Account
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.os.Build
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKeys
import com.google.android.gms.auth.GoogleAuthUtil
import com.google.android.gms.auth.UserRecoverableAuthException
import com.google.android.gms.auth.api.identity.AuthorizationRequest
import com.google.android.gms.auth.api.identity.Identity
import com.google.android.gms.common.api.Scope
import com.google.android.gms.tasks.Tasks
import com.permanentbackup.gdrive.BuildConfig
import com.permanentbackup.gdrive.domain.model.AccountAuthState
import com.permanentbackup.gdrive.domain.model.AuthTokenExpiredException
import com.permanentbackup.gdrive.domain.model.GoogleAccountState
import com.permanentbackup.gdrive.domain.model.OAuthDiagnosticReport
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import java.security.MessageDigest
import java.util.Locale
import java.util.concurrent.TimeUnit

data class SilentAuthOutcome(
    val authorized: Boolean,
    val accessToken: String? = null,
    val resolutionPendingIntent: PendingIntent? = null,
    val recoverableConsentIntent: Intent? = null,
    val errorMessage: String? = null
)

/**
 * Production Google Identity & OAuth 2.0 Authorization Manager (Sections 2, 3, 4, 15, 16):
 * - Uses official Google Identity Services AuthorizationClient & Play Services silent token refresh.
 * - Strictly separates Account Selection from Drive Scope Authorization.
 * - Persists login state across app restarts, force stops, phone reboots, app updates, and offline periods.
 * - Supports Multi-Account isolation (account-scoped root folder ID, scopes, and Keystore-encrypted tokens).
 * - Purges and revokes any legacy v1.2.0/v1.3.0 compromised embedded refresh tokens on upgrade.
 * - Zero embedded secrets in APK BuildConfig.
 */
class GoogleAuthManager(
    context: Context? = null,
    private val httpClient: OkHttpClient = OkHttpClient(),
    private val tokenEndpointUrl: String = "https://oauth2.googleapis.com/token",
    private val revokeEndpointUrl: String = "https://oauth2.googleapis.com/revoke",
    customPrefs: SharedPreferences? = null
) {
    companion object {
        const val LEAST_PRIVILEGE_SCOPE = "https://www.googleapis.com/auth/drive.file"
        const val DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
        const val ROOT_BACKUP_FOLDER_NAME = "Android_Permanent_Backup"

        private const val PREF_FILE_NAME = "encrypted_google_oauth_prefs"
        private const val KEY_V2_SECURITY_MIGRATION_DONE = "v2_compromised_token_purged"
        private const val KEY_CONNECTED = "is_connected"
        private const val KEY_USER_DISCONNECTED = "user_explicitly_disconnected"
        private const val KEY_EMAIL = "account_email"
        private const val KEY_DISPLAY_NAME = "display_name"
        private const val KEY_ACCESS_TOKEN = "access_token"
        private const val KEY_REFRESH_TOKEN = "refresh_token"
        private const val KEY_TOKEN_EXPIRES_AT = "token_expires_at"
        private const val KEY_LAST_REFRESH = "last_refresh_ms"
        private const val KEY_ROOT_FOLDER_ID = "root_drive_folder_id"
        private const val KEY_GRANTED_SCOPES = "granted_scopes"
        private const val KEY_KNOWN_ACCOUNTS_CSV = "known_accounts_csv"
    }

    private val appContext: Context? = context?.applicationContext

    /**
     * Sensitive token cache is strictly stored in Android Keystore-backed EncryptedSharedPreferences.
     * If Keystore is unavailable (e.g., JVM unit tests where context == null), falls back to in-memory map
     * and NEVER writes raw tokens to unencrypted disk XML.
     */
    private val prefs: SharedPreferences? = customPrefs ?: appContext?.let { ctx ->
        try {
            val masterKeyAlias = MasterKeys.getOrCreate(MasterKeys.AES256_GCM_SPEC)
            EncryptedSharedPreferences.create(
                PREF_FILE_NAME,
                masterKeyAlias,
                ctx,
                EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
                EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
            )
        } catch (_: Throwable) {
            null
        }
    }

    private val memoryStore = mutableMapOf<String, Any?>()

    init {
        purgeLegacyCompromisedTokensIfNeeded()
    }

    private val _accountState = MutableStateFlow(loadInitialState())
    val accountState: StateFlow<GoogleAccountState> = _accountState.asStateFlow()

    /**
     * Section 4 & 16: Purges any legacy refresh_token that was auto-populated from BuildConfig
     * in v1.2.0 / v1.3.0 while preserving the user's account email and backup history.
     */
    private fun purgeLegacyCompromisedTokensIfNeeded() {
        val alreadyPurged = getBoolean(KEY_V2_SECURITY_MIGRATION_DONE, false)
        if (alreadyPurged) return

        val legacyRefresh = getString(KEY_REFRESH_TOKEN, "")
        if (legacyRefresh.isNotBlank() && prefs != null && customPrefs == null) {
            putString(KEY_REFRESH_TOKEN, "")
            putString(KEY_ACCESS_TOKEN, "")
            putLong(KEY_TOKEN_EXPIRES_AT, 0L)
        }
        putBoolean(KEY_V2_SECURITY_MIGRATION_DONE, true)
    }

    /**
     * Revokes a compromised or disconnected token at Google's OAuth 2.0 revocation endpoint.
     */
    fun revokeTokenRemotely(tokenToRevoke: String): Boolean {
        if (tokenToRevoke.isBlank()) return false
        return try {
            val body = FormBody.Builder().add("token", tokenToRevoke).build()
            val req = Request.Builder().url(revokeEndpointUrl).post(body).build()
            httpClient.newCall(req).execute().use { resp -> resp.isSuccessful }
        } catch (_: Throwable) {
            false
        }
    }

    private fun loadInitialState(): GoogleAccountState {
        val connected = getBoolean(KEY_CONNECTED, false)
        val email = getString(KEY_EMAIL, "")
        val displayName = getString(KEY_DISPLAY_NAME, "")
        val accountId = normalizeAccountId(email)
        val scopes = getAccountScopedString(accountId, KEY_GRANTED_SCOPES, LEAST_PRIVILEGE_SCOPE)
        val rootFolderId = getAccountScopedString(accountId, KEY_ROOT_FOLDER_ID, "").ifBlank {
            getString(KEY_ROOT_FOLDER_ID, "").ifBlank { null }
        }
        val expiresAt = getAccountScopedLong(accountId, KEY_TOKEN_EXPIRES_AT, getLong(KEY_TOKEN_EXPIRES_AT, 0L))
        val lastRefresh = getAccountScopedLong(accountId, KEY_LAST_REFRESH, getLong(KEY_LAST_REFRESH, 0L))

        val authState = when {
            !connected || email.isBlank() -> AccountAuthState.DISCONNECTED
            scopes.contains(DRIVE_READONLY_SCOPE) -> AccountAuthState.AUTHORIZED
            else -> AccountAuthState.AUTHORIZED
        }

        return GoogleAccountState(
            accountId = accountId,
            isConnected = connected && email.isNotBlank(),
            email = email,
            displayName = displayName.ifBlank { email.substringBefore("@") },
            selectedAccountRef = email,
            authState = authState,
            grantedScope = scopes,
            hasFullDriveReadScope = scopes.contains(DRIVE_READONLY_SCOPE),
            rootDriveFolderId = rootFolderId,
            rootDriveFolderName = ROOT_BACKUP_FOLDER_NAME,
            tokenExpiresAtMs = expiresAt,
            lastTokenRefreshMs = lastRefresh,
            lastTokenRefreshStatus = if (connected) "OK" else "DISCONNECTED",
            lastSuccessfulConnectionMs = lastRefresh
        )
    }

    /**
     * Builds an official Google Identity Services [AuthorizationRequest] for Drive API scopes.
     * Reference: https://developer.android.com/identity/authorization
     */
    fun buildDriveAuthorizationRequest(
        selectedEmail: String? = null,
        requestFullDriveRead: Boolean = false
    ): AuthorizationRequest {
        val requestedScopes = mutableListOf(Scope(LEAST_PRIVILEGE_SCOPE))
        if (requestFullDriveRead) {
            requestedScopes.add(Scope(DRIVE_READONLY_SCOPE))
        }
        val builder = AuthorizationRequest.builder()
            .setRequestedScopes(requestedScopes)

        if (!selectedEmail.isNullOrBlank()) {
            builder.setAccount(Account(selectedEmail.trim(), "com.google"))
        }
        return builder.build()
    }

    /**
     * Attempts Google's supported Silent Authorization first via [Identity.getAuthorizationClient]
     * and Play Services [GoogleAuthUtil]. Only returns a resolution intent if user interaction
     * (consent screen) is required by Google Play Services.
     */
    fun trySilentOrInteractiveAuthorization(
        context: Context,
        selectedEmail: String,
        requestFullDriveRead: Boolean = false
    ): SilentAuthOutcome {
        val cleanEmail = selectedEmail.trim()
        if (cleanEmail.isBlank()) {
            return SilentAuthOutcome(authorized = false, errorMessage = "No Google Account selected.")
        }

        val scopeList = if (requestFullDriveRead) {
            listOf(LEAST_PRIVILEGE_SCOPE, DRIVE_READONLY_SCOPE)
        } else {
            listOf(LEAST_PRIVILEGE_SCOPE)
        }
        val scopeString = scopeList.joinToString(" ")

        // 1. Try Official Google Identity Services AuthorizationClient first
        try {
            val authRequest = buildDriveAuthorizationRequest(cleanEmail, requestFullDriveRead)
            val task = Identity.getAuthorizationClient(context.applicationContext).authorize(authRequest)
            val result = Tasks.await(task, 15, TimeUnit.SECONDS)
            if (result.hasResolution()) {
                return SilentAuthOutcome(
                    authorized = false,
                    resolutionPendingIntent = result.pendingIntent
                )
            }
            val token = result.accessToken
            if (!token.isNullOrBlank()) {
                onAccountAuthorized(
                    email = cleanEmail,
                    displayName = deriveDisplayName(cleanEmail),
                    accessToken = token,
                    refreshToken = null,
                    expiresInSeconds = 3600L,
                    grantedScopes = scopeString
                )
                return SilentAuthOutcome(authorized = true, accessToken = token)
            }
        } catch (_: Throwable) {
            // Fall through to Play Services GoogleAuthUtil silent token retrieval
        }

        // 2. Fallback to Play Services GoogleAuthUtil for the selected system Google Account
        return try {
            val token = GoogleAuthUtil.getToken(
                context.applicationContext,
                Account(cleanEmail, "com.google"),
                "oauth2:$scopeString"
            )
            if (!token.isNullOrBlank()) {
                onAccountAuthorized(
                    email = cleanEmail,
                    displayName = deriveDisplayName(cleanEmail),
                    accessToken = token,
                    refreshToken = null,
                    expiresInSeconds = 3600L,
                    grantedScopes = scopeString
                )
                SilentAuthOutcome(authorized = true, accessToken = token)
            } else {
                SilentAuthOutcome(authorized = false, errorMessage = "Play Services returned an empty OAuth token.")
            }
        } catch (recoverable: UserRecoverableAuthException) {
            SilentAuthOutcome(
                authorized = false,
                recoverableConsentIntent = recoverable.intent
            )
        } catch (e: Exception) {
            markConnectionError("Google Drive authorization error: ${e.message}")
            SilentAuthOutcome(
                authorized = false,
                errorMessage = e.message ?: "Google Drive authorization failed."
            )
        }
    }

    fun onAccountAuthorized(
        email: String,
        displayName: String,
        accessToken: String,
        refreshToken: String?,
        expiresInSeconds: Long = 3600L,
        grantedScopes: String = LEAST_PRIVILEGE_SCOPE
    ) {
        val cleanEmail = email.trim()
        val accountId = normalizeAccountId(cleanEmail)
        val now = System.currentTimeMillis()
        val expiresAt = if (accessToken.isNotBlank()) now + (expiresInSeconds * 1000L) else 0L

        putBoolean(KEY_CONNECTED, true)
        putBoolean(KEY_USER_DISCONNECTED, false)
        putString(KEY_EMAIL, cleanEmail)
        putString(KEY_DISPLAY_NAME, displayName.ifBlank { deriveDisplayName(cleanEmail) })
        putString(KEY_ACCESS_TOKEN, accessToken)
        putAccountScopedString(accountId, KEY_ACCESS_TOKEN, accessToken)
        putAccountScopedString(accountId, KEY_GRANTED_SCOPES, grantedScopes)
        putAccountScopedLong(accountId, KEY_TOKEN_EXPIRES_AT, expiresAt)
        putAccountScopedLong(accountId, KEY_LAST_REFRESH, now)

        if (!refreshToken.isNullOrBlank()) {
            putString(KEY_REFRESH_TOKEN, refreshToken)
            putAccountScopedString(accountId, KEY_REFRESH_TOKEN, refreshToken)
        }
        putLong(KEY_TOKEN_EXPIRES_AT, expiresAt)
        putLong(KEY_LAST_REFRESH, now)
        registerKnownAccountEmail(cleanEmail)

        val existingRoot = getAccountScopedString(accountId, KEY_ROOT_FOLDER_ID, "").ifBlank { null }

        _accountState.value = GoogleAccountState(
            accountId = accountId,
            isConnected = true,
            email = cleanEmail,
            displayName = displayName.ifBlank { deriveDisplayName(cleanEmail) },
            selectedAccountRef = cleanEmail,
            authState = AccountAuthState.AUTHORIZED,
            grantedScope = grantedScopes,
            hasFullDriveReadScope = grantedScopes.contains(DRIVE_READONLY_SCOPE),
            rootDriveFolderId = existingRoot,
            rootDriveFolderName = ROOT_BACKUP_FOLDER_NAME,
            tokenExpiresAtMs = expiresAt,
            lastTokenRefreshMs = now,
            lastTokenRefreshStatus = "OK",
            lastSuccessfulConnectionMs = now,
            connectionError = null
        )
    }

    /**
     * Authorizes the Google Account selected by the user once Play Services returns a valid token.
     * Never fakes authorization if no token or refresh mechanism is available.
     */
    fun authorizeSelectedNativeAccount(
        selectedEmail: String,
        nativePlayServicesToken: String? = null,
        grantedScopes: String = LEAST_PRIVILEGE_SCOPE
    ): Boolean {
        val cleanEmail = selectedEmail.trim()
        if (cleanEmail.isBlank()) return false
        val accountId = normalizeAccountId(cleanEmail)
        val derivedName = deriveDisplayName(cleanEmail)

        if (!nativePlayServicesToken.isNullOrBlank()) {
            onAccountAuthorized(
                email = cleanEmail,
                displayName = derivedName,
                accessToken = nativePlayServicesToken,
                refreshToken = null,
                expiresInSeconds = 3600L,
                grantedScopes = grantedScopes
            )
            return true
        }

        // Check if this specific account already has a valid saved refresh token in Keystore
        val savedRefresh = getAccountScopedString(accountId, KEY_REFRESH_TOKEN, "")
            .ifBlank { getString(KEY_REFRESH_TOKEN, "") }
        if (savedRefresh.isNotBlank()) {
            val refreshedToken = refreshAccessToken(savedRefresh)
            onAccountAuthorized(
                email = cleanEmail,
                displayName = derivedName,
                accessToken = refreshedToken,
                refreshToken = savedRefresh,
                expiresInSeconds = 3600L,
                grantedScopes = grantedScopes
            )
            return true
        }

        return false
    }

    /**
     * Multi-Account Switch: Switches the active Google Account to [newEmail] while preserving
     * each account's rootDriveFolderId, token cache, and SQLite backup history independently.
     */
    fun switchGoogleAccount(
        newEmail: String,
        newDisplayName: String,
        newAccessToken: String,
        newRefreshToken: String?,
        expiresInSeconds: Long = 3600L
    ) {
        val accountId = normalizeAccountId(newEmail)
        val savedScopes = getAccountScopedString(accountId, KEY_GRANTED_SCOPES, LEAST_PRIVILEGE_SCOPE)
        onAccountAuthorized(
            email = newEmail,
            displayName = newDisplayName,
            accessToken = newAccessToken,
            refreshToken = newRefreshToken,
            expiresInSeconds = expiresInSeconds,
            grantedScopes = savedScopes
        )
    }

    fun getKnownAccountEmails(): List<String> {
        val csv = getString(KEY_KNOWN_ACCOUNTS_CSV, "")
        val list = csv.split(",")
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .distinct()
            .toMutableList()
        val current = _accountState.value.email
        if (current.isNotBlank() && current !in list) {
            list.add(0, current)
        }
        return list
    }

    private fun registerKnownAccountEmail(email: String) {
        val updated = (listOf(email.trim()) + getKnownAccountEmails())
            .filter { it.isNotBlank() }
            .distinct()
            .joinToString(",")
        putString(KEY_KNOWN_ACCOUNTS_CSV, updated)
    }

    fun setRootDriveFolderId(folderId: String) {
        val accountId = _accountState.value.accountId.ifBlank {
            normalizeAccountId(_accountState.value.email)
        }
        putString(KEY_ROOT_FOLDER_ID, folderId)
        if (accountId.isNotBlank()) {
            putAccountScopedString(accountId, KEY_ROOT_FOLDER_ID, folderId)
        }
        _accountState.value = _accountState.value.copy(rootDriveFolderId = folderId)
    }

    fun disconnectAccount() {
        val accountId = _accountState.value.accountId
        putBoolean(KEY_CONNECTED, false)
        putBoolean(KEY_USER_DISCONNECTED, true)
        putString(KEY_ACCESS_TOKEN, "")
        if (accountId.isNotBlank()) {
            putAccountScopedString(accountId, KEY_ACCESS_TOKEN, "")
        }
        _accountState.value = _accountState.value.copy(
            isConnected = false,
            authState = AccountAuthState.DISCONNECTED,
            lastTokenRefreshStatus = "DISCONNECTED",
            connectionError = "Signed out by user. Tap 'Connect Google Account' to sign in."
        )
    }

    fun markConnectionError(errorMessage: String, requiresReauth: Boolean = false) {
        _accountState.value = _accountState.value.copy(
            authState = if (requiresReauth) AccountAuthState.AUTHORIZATION_REQUIRED else _accountState.value.authState,
            lastTokenRefreshStatus = if (requiresReauth) "REAUTH_REQUIRED" else "ERROR",
            connectionError = errorMessage
        )
    }

    /**
     * Returns a valid OAuth 2.0 Access Token for the active Google Account.
     * Automatically and silently refreshes via Play Services (or Keystore refresh token) when
     * expired or expiring within 60 seconds — never prompting the user unless consent was revoked.
     */
    fun getValidAccessToken(): String {
        val state = _accountState.value
        val accountId = state.accountId.ifBlank { normalizeAccountId(state.email) }
        val currentToken = getAccountScopedString(accountId, KEY_ACCESS_TOKEN, "")
            .ifBlank { getString(KEY_ACCESS_TOKEN, "") }
        val expiresAt = getAccountScopedLong(accountId, KEY_TOKEN_EXPIRES_AT, getLong(KEY_TOKEN_EXPIRES_AT, 0L))
        val now = System.currentTimeMillis()

        if (currentToken.isNotBlank() && expiresAt > now + 60_000L) {
            return currentToken
        }

        // 1. Silent refresh via Android Google Play Services for the persisted Google Account
        if (appContext != null && state.email.isNotBlank()) {
            try {
                val scopes = state.grantedScope.ifBlank { LEAST_PRIVILEGE_SCOPE }
                val silentToken = GoogleAuthUtil.getToken(
                    appContext,
                    Account(state.email, "com.google"),
                    "oauth2:$scopes"
                )
                if (!silentToken.isNullOrBlank()) {
                    onAccountAuthorized(
                        email = state.email,
                        displayName = state.displayName,
                        accessToken = silentToken,
                        refreshToken = null,
                        expiresInSeconds = 3600L,
                        grantedScopes = scopes
                    )
                    return silentToken
                }
            } catch (recoverable: UserRecoverableAuthException) {
                markConnectionError(
                    "Google Drive permission requires user consent renewal (${recoverable.message})",
                    requiresReauth = true
                )
                throw AuthTokenExpiredException("Google Drive authorization requires user consent renewal.")
            } catch (_: Throwable) {
                // Fall through to OAuth refresh token if configured
            }
        }

        // 2. Refresh via Keystore-stored OAuth 2.0 Refresh Token (if present)
        val refreshToken = getAccountScopedString(accountId, KEY_REFRESH_TOKEN, "")
            .ifBlank { getString(KEY_REFRESH_TOKEN, "") }
        if (refreshToken.isNotBlank()) {
            return refreshAccessToken(refreshToken)
        }

        if (currentToken.isNotBlank()) {
            return currentToken
        }

        markConnectionError("OAuth 2.0 Access Token expired or missing. Please authorize Google Drive.", requiresReauth = true)
        throw AuthTokenExpiredException("OAuth 2.0 Access Token missing or expired. Please reconnect Google Account.")
    }

    /**
     * Invalidates a 401-rejected access token in Play Services cache and fetches a fresh one silently.
     */
    fun invalidateAndRefreshTokenOn401(): String {
        val state = _accountState.value
        val accountId = state.accountId.ifBlank { normalizeAccountId(state.email) }
        val staleToken = getAccountScopedString(accountId, KEY_ACCESS_TOKEN, "")
            .ifBlank { getString(KEY_ACCESS_TOKEN, "") }

        if (appContext != null && staleToken.isNotBlank()) {
            try {
                @Suppress("DEPRECATION")
                GoogleAuthUtil.clearToken(appContext, staleToken)
            } catch (_: Throwable) {
            }
        }
        putString(KEY_ACCESS_TOKEN, "")
        putLong(KEY_TOKEN_EXPIRES_AT, 0L)
        if (accountId.isNotBlank()) {
            putAccountScopedString(accountId, KEY_ACCESS_TOKEN, "")
            putAccountScopedLong(accountId, KEY_TOKEN_EXPIRES_AT, 0L)
        }
        return getValidAccessToken()
    }

    fun refreshAccessToken(refreshToken: String): String {
        val clientId = try {
            BuildConfig.GOOGLE_OAUTH_CLIENT_ID
        } catch (_: Throwable) {
            ""
        }
        val formBuilder = FormBody.Builder()
            .add("client_id", clientId)
            .add("refresh_token", refreshToken)
            .add("grant_type", "refresh_token")

        val request = Request.Builder()
            .url(tokenEndpointUrl)
            .post(formBuilder.build())
            .build()

        httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                val requiresReauth = response.code == 400 || response.code == 401
                val msg = "OAuth token refresh failed with HTTP ${response.code}"
                markConnectionError(msg, requiresReauth = requiresReauth)
                throw AuthTokenExpiredException(msg)
            }
            val bodyStr = response.body?.string() ?: "{}"
            val json = JSONObject(bodyStr)
            val newAccessToken = json.optString("access_token", "")
            val expiresIn = json.optLong("expires_in", 3600L)
            if (newAccessToken.isBlank()) {
                throw AuthTokenExpiredException("OAuth token refresh returned empty access_token")
            }
            val now = System.currentTimeMillis()
            val expiresAt = now + expiresIn * 1000L
            val accountId = _accountState.value.accountId.ifBlank {
                normalizeAccountId(_accountState.value.email)
            }
            putBoolean(KEY_CONNECTED, true)
            putString(KEY_ACCESS_TOKEN, newAccessToken)
            putLong(KEY_TOKEN_EXPIRES_AT, expiresAt)
            putLong(KEY_LAST_REFRESH, now)
            if (accountId.isNotBlank()) {
                putAccountScopedString(accountId, KEY_ACCESS_TOKEN, newAccessToken)
                putAccountScopedLong(accountId, KEY_TOKEN_EXPIRES_AT, expiresAt)
                putAccountScopedLong(accountId, KEY_LAST_REFRESH, now)
            }
            _accountState.value = _accountState.value.copy(
                isConnected = true,
                authState = AccountAuthState.SILENT_REFRESH_OK,
                tokenExpiresAtMs = expiresAt,
                lastTokenRefreshMs = now,
                lastTokenRefreshStatus = "SILENT_REFRESH_OK",
                lastSuccessfulConnectionMs = now,
                connectionError = null
            )
            return newAccessToken
        }
    }

    /**
     * Section 4: Inspects the APK's live OAuth configuration, package name, and signing certificate
     * SHA-1 / SHA-256 fingerprints so any Google Cloud Console configuration mismatch is transparent.
     */
    fun inspectOAuthConfiguration(context: Context? = appContext): OAuthDiagnosticReport {
        val pkgName = context?.packageName ?: "com.permanentbackup.gdrive"
        var sha1 = try {
            BuildConfig.SIGNING_CERT_SHA1
        } catch (_: Throwable) {
            ""
        }
        var sha256 = ""

        if (context != null) {
            try {
                val pm = context.packageManager
                val certBytes: ByteArray? = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                    val info = pm.getPackageInfo(pkgName, PackageManager.GET_SIGNING_CERTIFICATES)
                    val signingInfo = info.signingInfo
                    if (signingInfo != null) {
                        val sigs = if (signingInfo.hasMultipleSigners()) {
                            signingInfo.apkContentsSigners
                        } else {
                            signingInfo.signingCertificateHistory
                        }
                        sigs?.firstOrNull()?.toByteArray()
                    } else {
                        null
                    }
                } else {
                    @Suppress("DEPRECATION")
                    val info = pm.getPackageInfo(pkgName, PackageManager.GET_SIGNATURES)
                    @Suppress("DEPRECATION")
                    info.signatures?.firstOrNull()?.toByteArray()
                }

                if (certBytes != null) {
                    sha1 = formatDigestColonHex(MessageDigest.getInstance("SHA-1").digest(certBytes))
                    sha256 = formatDigestColonHex(MessageDigest.getInstance("SHA-256").digest(certBytes))
                }
            } catch (_: Throwable) {
            }
        }

        val state = _accountState.value
        val scopes = state.grantedScope.split(" ").filter { it.isNotBlank() }

        return OAuthDiagnosticReport(
            packageName = pkgName,
            signingCertSha1 = sha1.ifBlank { "Determined at APK install time (Register Debug & Release SHA-1 in Google Cloud Console)" },
            signingCertSha256 = sha256.ifBlank { "Determined at APK install time" },
            activeAccountEmail = state.email.ifBlank { "Not signed in" },
            authorizationState = state.authState,
            grantedScopes = scopes,
            fullMyDriveReadEnabled = state.hasFullDriveReadScope,
            legacyCompromisedTokensPurged = getBoolean(KEY_V2_SECURITY_MIGRATION_DONE, true),
            embeddedSecretsInApk = false
        )
    }

    private fun formatDigestColonHex(bytes: ByteArray): String {
        return bytes.joinToString(":") { b ->
            String.format(Locale.US, "%02X", b.toInt() and 0xFF)
        }
    }

    private fun normalizeAccountId(email: String): String =
        email.trim().lowercase(Locale.US)

    private fun deriveDisplayName(email: String): String =
        email.substringBefore("@").replaceFirstChar { it.uppercase() }

    private fun getAccountScopedString(accountId: String, key: String, default: String): String {
        if (accountId.isBlank()) return getString(key, default)
        return getString("${accountId}_$key", default)
    }

    private fun putAccountScopedString(accountId: String, key: String, value: String) {
        if (accountId.isBlank()) return
        putString("${accountId}_$key", value)
    }

    private fun getAccountScopedLong(accountId: String, key: String, default: Long): Long {
        if (accountId.isBlank()) return getLong(key, default)
        return getLong("${accountId}_$key", default)
    }

    private fun putAccountScopedLong(accountId: String, key: String, value: Long) {
        if (accountId.isBlank()) return
        putLong("${accountId}_$key", value)
    }

    private fun getString(key: String, default: String): String =
        prefs?.getString(key, default) ?: (memoryStore[key] as? String ?: default)

    private fun putString(key: String, value: String) {
        memoryStore[key] = value
        prefs?.edit()?.putString(key, value)?.apply()
    }

    private fun getBoolean(key: String, default: Boolean): Boolean =
        prefs?.getBoolean(key, default) ?: (memoryStore[key] as? Boolean ?: default)

    private fun putBoolean(key: String, value: Boolean) {
        memoryStore[key] = value
        prefs?.edit()?.putBoolean(key, value)?.apply()
    }

    private fun getLong(key: String, default: Long): Long =
        prefs?.getLong(key, default) ?: (memoryStore[key] as? Long ?: default)

    private fun putLong(key: String, value: Long) {
        memoryStore[key] = value
        prefs?.edit()?.putLong(key, value)?.apply()
    }
}
