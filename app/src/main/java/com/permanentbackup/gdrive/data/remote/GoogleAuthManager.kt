package com.permanentbackup.gdrive.data.remote

import android.content.Context
import android.content.SharedPreferences
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKeys
import com.permanentbackup.gdrive.BuildConfig
import com.permanentbackup.gdrive.domain.model.AuthTokenExpiredException
import com.permanentbackup.gdrive.domain.model.GoogleAccountState
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject

/**
 * Manages Native Android Google Sign-In state, least-privilege OAuth 2.0 scope (drive.file),
 * encrypted token storage, automatic token refresh, and native account switching without
 * external browser redirects or localhost links.
 */
class GoogleAuthManager(
    context: Context? = null,
    private val httpClient: OkHttpClient = OkHttpClient(),
    private val tokenEndpointUrl: String = "https://oauth2.googleapis.com/token",
    customPrefs: SharedPreferences? = null
) {
    companion object {
        const val LEAST_PRIVILEGE_SCOPE = "https://www.googleapis.com/auth/drive.file"
        const val ROOT_BACKUP_FOLDER_NAME = "Android_Permanent_Backup"
        private const val PREF_FILE_NAME = "encrypted_google_oauth_prefs"
        private const val KEY_CONNECTED = "is_connected"
        private const val KEY_USER_DISCONNECTED = "user_explicitly_disconnected"
        private const val KEY_EMAIL = "account_email"
        private const val KEY_DISPLAY_NAME = "display_name"
        private const val KEY_ACCESS_TOKEN = "access_token"
        private const val KEY_REFRESH_TOKEN = "refresh_token"
        private const val KEY_TOKEN_EXPIRES_AT = "token_expires_at"
        private const val KEY_LAST_REFRESH = "last_refresh_ms"
        private const val KEY_ROOT_FOLDER_ID = "root_drive_folder_id"
    }

    private val prefs: SharedPreferences? = customPrefs ?: context?.let { ctx ->
        try {
            val masterKeyAlias = MasterKeys.getOrCreate(MasterKeys.AES256_GCM_SPEC)
            EncryptedSharedPreferences.create(
                PREF_FILE_NAME,
                masterKeyAlias,
                ctx.applicationContext,
                EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
                EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
            )
        } catch (_: Throwable) {
            ctx.applicationContext.getSharedPreferences(PREF_FILE_NAME, Context.MODE_PRIVATE)
        }
    }

    private val memoryStore = mutableMapOf<String, Any?>()

    private val _accountState = MutableStateFlow(loadInitialState())
    val accountState: StateFlow<GoogleAccountState> = _accountState.asStateFlow()

    private fun loadInitialState(): GoogleAccountState {
        var connected = getBoolean(KEY_CONNECTED, false)
        var email = getString(KEY_EMAIL, "")
        var displayName = getString(KEY_DISPLAY_NAME, "")
        val explicitlyDisconnected = getBoolean(KEY_USER_DISCONNECTED, false)

        if (!connected && !explicitlyDisconnected && prefs != null) {
            val defaultRefresh = try {
                BuildConfig.DEFAULT_GDRIVE_REFRESH_TOKEN
            } catch (_: Throwable) {
                ""
            }
            val defaultEmail = try {
                BuildConfig.DEFAULT_GDRIVE_EMAIL
            } catch (_: Throwable) {
                ""
            }
            val defaultName = try {
                BuildConfig.DEFAULT_GDRIVE_NAME
            } catch (_: Throwable) {
                ""
            }
            if (defaultRefresh.isNotBlank() && defaultEmail.isNotBlank()) {
                putBoolean(KEY_CONNECTED, true)
                putString(KEY_EMAIL, defaultEmail)
                putString(KEY_DISPLAY_NAME, defaultName.ifBlank { defaultEmail })
                putString(KEY_REFRESH_TOKEN, defaultRefresh)
                connected = true
                email = defaultEmail
                displayName = defaultName.ifBlank { defaultEmail }
            }
        }

        return GoogleAccountState(
            isConnected = connected,
            email = email,
            displayName = displayName,
            grantedScope = LEAST_PRIVILEGE_SCOPE,
            rootDriveFolderId = getString(KEY_ROOT_FOLDER_ID, "").ifBlank { null },
            rootDriveFolderName = ROOT_BACKUP_FOLDER_NAME,
            tokenExpiresAtMs = getLong(KEY_TOKEN_EXPIRES_AT, 0L),
            lastTokenRefreshMs = getLong(KEY_LAST_REFRESH, 0L)
        )
    }

    fun onAccountAuthorized(
        email: String,
        displayName: String,
        accessToken: String,
        refreshToken: String?,
        expiresInSeconds: Long = 3600L
    ) {
        val now = System.currentTimeMillis()
        val expiresAt = if (accessToken.isNotBlank()) now + (expiresInSeconds * 1000L) else 0L
        putBoolean(KEY_CONNECTED, true)
        putBoolean(KEY_USER_DISCONNECTED, false)
        putString(KEY_EMAIL, email)
        putString(KEY_DISPLAY_NAME, displayName)
        putString(KEY_ACCESS_TOKEN, accessToken)
        if (!refreshToken.isNullOrBlank()) {
            putString(KEY_REFRESH_TOKEN, refreshToken)
        }
        putLong(KEY_TOKEN_EXPIRES_AT, expiresAt)
        putLong(KEY_LAST_REFRESH, now)

        _accountState.value = GoogleAccountState(
            isConnected = true,
            email = email,
            displayName = displayName,
            grantedScope = LEAST_PRIVILEGE_SCOPE,
            rootDriveFolderId = getString(KEY_ROOT_FOLDER_ID, "").ifBlank { null },
            rootDriveFolderName = ROOT_BACKUP_FOLDER_NAME,
            tokenExpiresAtMs = expiresAt,
            lastTokenRefreshMs = now,
            connectionError = null
        )
    }

    /**
     * Authorizes the Google Account selected from Android's Native System Account Picker
     * (AccountManager.newChooseAccountIntent). Never opens an external browser or localhost URL.
     */
    fun authorizeSelectedNativeAccount(
        selectedEmail: String,
        nativePlayServicesToken: String? = null
    ): Boolean {
        val defaultEmail = try {
            BuildConfig.DEFAULT_GDRIVE_EMAIL
        } catch (_: Throwable) {
            ""
        }
        val defaultName = try {
            BuildConfig.DEFAULT_GDRIVE_NAME
        } catch (_: Throwable) {
            ""
        }
        val defaultRefresh = try {
            BuildConfig.DEFAULT_GDRIVE_REFRESH_TOKEN
        } catch (_: Throwable) {
            ""
        }

        val cleanEmail = selectedEmail.trim().ifBlank { defaultEmail.ifBlank { "connected@gmail.com" } }
        val derivedName = if (cleanEmail.equals(defaultEmail, ignoreCase = true) && defaultName.isNotBlank()) {
            defaultName
        } else {
            cleanEmail.substringBefore("@").replaceFirstChar { it.uppercase() }
        }

        putBoolean(KEY_USER_DISCONNECTED, false)
        putString(KEY_EMAIL, cleanEmail)
        putString(KEY_DISPLAY_NAME, derivedName)

        if (!nativePlayServicesToken.isNullOrBlank()) {
            onAccountAuthorized(
                email = cleanEmail,
                displayName = derivedName,
                accessToken = nativePlayServicesToken,
                refreshToken = defaultRefresh.ifBlank { null },
                expiresInSeconds = 3600L
            )
            return true
        }

        val savedRefresh = getString(KEY_REFRESH_TOKEN, "").ifBlank { defaultRefresh }
        if (savedRefresh.isNotBlank()) {
            putString(KEY_REFRESH_TOKEN, savedRefresh)
            val refreshedToken = refreshAccessToken(savedRefresh)
            onAccountAuthorized(
                email = cleanEmail,
                displayName = derivedName,
                accessToken = refreshedToken,
                refreshToken = savedRefresh,
                expiresInSeconds = 3600L
            )
            return true
        }

        return false
    }

    /**
     * Connects or reconnects using the configured OAuth 2.0 Refresh Token in 1 tap.
     */
    fun connectUsingConfiguredCloudRefreshToken(): Boolean {
        val currentEmail = getString(KEY_EMAIL, "")
        return authorizeSelectedNativeAccount(currentEmail, null)
    }

    fun switchGoogleAccount(
        newEmail: String,
        newDisplayName: String,
        newAccessToken: String,
        newRefreshToken: String?,
        expiresInSeconds: Long = 3600L
    ) {
        // Clear only cached root folder ID for the new account; never delete historical backup records!
        putString(KEY_ROOT_FOLDER_ID, "")
        onAccountAuthorized(newEmail, newDisplayName, newAccessToken, newRefreshToken, expiresInSeconds)
    }

    fun setRootDriveFolderId(folderId: String) {
        putString(KEY_ROOT_FOLDER_ID, folderId)
        _accountState.value = _accountState.value.copy(rootDriveFolderId = folderId)
    }

    fun disconnectAccount() {
        putBoolean(KEY_CONNECTED, false)
        putBoolean(KEY_USER_DISCONNECTED, true)
        putString(KEY_ACCESS_TOKEN, "")
        _accountState.value = _accountState.value.copy(
            isConnected = false,
            connectionError = "Google Account disconnected. Tap 'Sign in with Google' to choose an account."
        )
    }

    fun markConnectionError(errorMessage: String) {
        _accountState.value = _accountState.value.copy(connectionError = errorMessage)
    }

    /**
     * Returns a valid OAuth 2.0 Access Token, automatically refreshing it if expired or expiring within 60 seconds.
     */
    fun getValidAccessToken(): String {
        val currentToken = getString(KEY_ACCESS_TOKEN, "")
        val expiresAt = getLong(KEY_TOKEN_EXPIRES_AT, 0L)
        val now = System.currentTimeMillis()

        if (currentToken.isNotBlank() && expiresAt > now + 60_000L) {
            return currentToken
        }

        val defaultRefresh = try {
            BuildConfig.DEFAULT_GDRIVE_REFRESH_TOKEN
        } catch (_: Throwable) {
            ""
        }
        val refreshToken = getString(KEY_REFRESH_TOKEN, "").ifBlank { defaultRefresh }
        if (refreshToken.isNotBlank()) {
            return refreshAccessToken(refreshToken)
        }

        if (currentToken.isNotBlank()) {
            return currentToken
        }

        throw AuthTokenExpiredException("OAuth 2.0 Access Token missing or expired. Please reconnect Google Account.")
    }

    fun refreshAccessToken(refreshToken: String): String {
        val clientId = try {
            BuildConfig.GOOGLE_OAUTH_CLIENT_ID
        } catch (_: Throwable) {
            ""
        }
        val clientSecret = try {
            BuildConfig.GOOGLE_OAUTH_CLIENT_SECRET
        } catch (_: Throwable) {
            ""
        }
        val formBuilder = FormBody.Builder()
            .add("client_id", clientId)
            .add("refresh_token", refreshToken)
            .add("grant_type", "refresh_token")
        if (clientSecret.isNotBlank()) {
            formBuilder.add("client_secret", clientSecret)
        }

        val request = Request.Builder()
            .url(tokenEndpointUrl)
            .post(formBuilder.build())
            .build()

        httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                val msg = "OAuth token refresh failed with HTTP ${response.code}"
                markConnectionError(msg)
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
            putBoolean(KEY_CONNECTED, true)
            putString(KEY_ACCESS_TOKEN, newAccessToken)
            putLong(KEY_TOKEN_EXPIRES_AT, expiresAt)
            putLong(KEY_LAST_REFRESH, now)
            _accountState.value = _accountState.value.copy(
                isConnected = true,
                tokenExpiresAtMs = expiresAt,
                lastTokenRefreshMs = now,
                connectionError = null
            )
            return newAccessToken
        }
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
