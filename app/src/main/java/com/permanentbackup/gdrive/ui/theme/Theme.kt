package com.permanentbackup.gdrive.ui.theme

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color

private val PermanentColorScheme = lightColorScheme(
    primary = Color(0xFF1A73E8),
    onPrimary = Color.White,
    primaryContainer = Color(0xFFD2E3FC),
    onPrimaryContainer = Color(0xFF041E49),
    secondary = Color(0xFF137333),
    onSecondary = Color.White,
    secondaryContainer = Color(0xFFCEEAD6),
    error = Color(0xFFC5221F),
    background = Color(0xFFF8FAFD),
    surface = Color.White
)

@Composable
fun PermanentBackupTheme(content: @Composable () -> Unit) {
    MaterialTheme(
        colorScheme = PermanentColorScheme,
        content = content
    )
}
