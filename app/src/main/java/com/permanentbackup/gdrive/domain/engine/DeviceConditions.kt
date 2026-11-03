package com.permanentbackup.gdrive.domain.engine

import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.BatteryManager
import com.permanentbackup.gdrive.domain.model.BackupSettings

/**
 * Result of checking whether the device currently satisfies the user's transfer conditions
 * (Wi-Fi / unmetered only, charging only). [blockReason] is a user-facing explanation when blocked.
 */
data class TransferConditionResult(
    val allowed: Boolean,
    val blockReason: String? = null
)

/**
 * Engine-level enforcement of Settings → "Wi-Fi only" / "Charging only".
 * WorkManager constraints only gate *scheduled* runs; manual, share-sheet, real-time watcher and
 * DocumentsProvider-triggered runs go straight to the engine, so the engine itself must check.
 */
interface DeviceConditionsProvider {
    fun evaluate(settings: BackupSettings): TransferConditionResult
}

/** JVM tests / no-context default: conditions always satisfied. */
object AlwaysSatisfiedDeviceConditions : DeviceConditionsProvider {
    override fun evaluate(settings: BackupSettings): TransferConditionResult = TransferConditionResult(true)
}

class AndroidDeviceConditionsProvider(context: Context) : DeviceConditionsProvider {
    private val appContext = context.applicationContext

    override fun evaluate(settings: BackupSettings): TransferConditionResult {
        val needsUnmetered = settings.wifiOnly || settings.unmeteredNetworkOnly
        try {
            val cm = appContext.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
            val caps = cm?.activeNetwork?.let { cm.getNetworkCapabilities(it) }
            if (caps == null || !caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)) {
                return TransferConditionResult(false, "Waiting for Network: no internet connection.")
            }
            if (needsUnmetered && !caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_NOT_METERED)) {
                return TransferConditionResult(
                    false,
                    "Waiting for Wi-Fi: 'Wi-Fi only' is enabled in Settings and the current network is metered/mobile data."
                )
            }
        } catch (_: Throwable) {
            // If connectivity cannot be read, do not block; the HTTP layer will report real network errors.
        }
        if (settings.chargingOnly) {
            val charging = try {
                val bm = appContext.getSystemService(Context.BATTERY_SERVICE) as? BatteryManager
                if (bm != null) {
                    bm.isCharging
                } else {
                    val status = appContext.registerReceiver(null, IntentFilter(Intent.ACTION_BATTERY_CHANGED))
                        ?.getIntExtra(BatteryManager.EXTRA_STATUS, -1) ?: -1
                    status == BatteryManager.BATTERY_STATUS_CHARGING || status == BatteryManager.BATTERY_STATUS_FULL
                }
            } catch (_: Throwable) {
                true
            }
            if (!charging) {
                return TransferConditionResult(
                    false,
                    "Waiting for Charging: 'Charging only' is enabled in Settings."
                )
            }
        }
        return TransferConditionResult(true)
    }
}
