package util

import com.qf.bigdata.release.util.CommonUtil

/**
  * Spark UDF
  */
object QFUdf {
  /**
    * 年龄段
    */
  def getAgeRange(age:String)={
    var tseg = ""
    try {
      tseg = CommonUtil.getAgeRange(age)
    }catch {
      case ex:Exception => {
        println(s"$ex")
      }
    }
  }
}
