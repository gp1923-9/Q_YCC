package constant

import org.apache.spark.storage.StorageLevel

/**
  * 常量
  */
object ReleaseConstant {

  // partition
  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION:String = "bdp_day"
  val DEF_SOURCE_PARTITION = 4


  // 维度列
  val COL_RLEASE_SESSION_STATUS:String = "release_status"
  val COL_RELEASE_SOURCES = "sources"
  val COL_RELEASE_CHANNELS = "channels"
  val COL_RELEASE_DEVICE_TYPE = "device_type"
  val COL_RELEASE_DEVICE_NUM = "device_num"
  val COL_RELEASE_USER_COUNT = "user_count"
  val COL_RELEASE_TOTAL_COUNT = "total_count"

  // ods================================
  val ODS_RELEASE_SESSION = "gp23_ods.ods_01_release_session"

  // dw=================================
  val DW_RELEASE_CUSTOMER = "gp23_dwd.dw_release_customer"

  // dm=================================
  val DM_RELEASE_CUSTOMER_SOURCE = "gp23_dm.dm_customer_sources"
}
