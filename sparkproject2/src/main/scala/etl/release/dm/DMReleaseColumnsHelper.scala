package etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {
  /**
    * 目标客户
    */
  def selectDWReleaseCustomers():ArrayBuffer[String] = {
    var columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 渠道指标列
    */
  def selectDMCustomerSourceColumns():ArrayBuffer[String]= {
    var columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }

}
