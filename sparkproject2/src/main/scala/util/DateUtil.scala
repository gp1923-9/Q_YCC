package util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * 数据处理工具类
  */
object DateUtil {

  def dateFromat4String(date:String,fromater:String = "yyyy-MM-dd"):String={
    if(null == date){
      return null
    }
    val formatter = DateTimeFormatter.ofPattern(fromater)
    val datetime = LocalDate.parse(date,formatter)
    // 校验时间参数
    datetime.format(DateTimeFormatter.ofPattern(fromater))
  }

  def dateFromat4StringDiff(date:String,diff:Long,fromater:String = "yyyy-MM-dd"):String = {
    if(null == date){
      return null
    }
    val formatter = DateTimeFormatter.ofPattern(fromater)
    val datetime = LocalDate.parse(date,formatter)
    // 处理天的累加
    val resultDateTime = datetime.plusDays(diff)

    resultDateTime.format(DateTimeFormatter.ofPattern(fromater))
  }

}
