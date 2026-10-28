package com.permanentbackup.gdrive.data.local

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.TypeConverter
import androidx.room.TypeConverters
import com.permanentbackup.gdrive.domain.model.FailureReason
import com.permanentbackup.gdrive.domain.model.UploadStatus
import com.permanentbackup.gdrive.domain.model.VerificationStatus

class BackupTypeConverters {
    @TypeConverter
    fun fromUploadStatus(value: UploadStatus): String = value.name

    @TypeConverter
    fun toUploadStatus(value: String): UploadStatus = UploadStatus.valueOf(value)

    @TypeConverter
    fun fromVerificationStatus(value: VerificationStatus): String = value.name

    @TypeConverter
    fun toVerificationStatus(value: String): VerificationStatus = VerificationStatus.valueOf(value)

    @TypeConverter
    fun fromFailureReason(value: FailureReason): String = value.name

    @TypeConverter
    fun toFailureReason(value: String): FailureReason = FailureReason.valueOf(value)
}

@Database(
    entities = [BackupFileEntity::class, DriveFolderCacheEntity::class],
    version = 1,
    exportSchema = false
)
@TypeConverters(BackupTypeConverters::class)
abstract class PermanentBackupDatabase : RoomDatabase() {
    abstract fun backupDao(): BackupDao

    companion object {
        @Volatile
        private var INSTANCE: PermanentBackupDatabase? = null

        fun getInstance(context: Context): PermanentBackupDatabase {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: Room.databaseBuilder(
                    context.applicationContext,
                    PermanentBackupDatabase::class.java,
                    "permanent_gdrive_backup.db"
                )
                    .fallbackToDestructiveMigrationOnDowngrade()
                    .build()
                    .also { INSTANCE = it }
            }
        }
    }
}
