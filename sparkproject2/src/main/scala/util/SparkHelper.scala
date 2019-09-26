package util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spark工具类
  */
object SparkHelper {

  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  /**
    * 读取数据
    */
  def readTableData(spark:SparkSession,tableName:String,colNames:mutable.Seq[String]):DataFrame={
    import spark.implicits._
    // 获取数据
    val tableDF = spark.read.table(tableName)
      .selectExpr(colNames:_*)
    //
    tableDF
  }

  /**
    * 写入数据
    */
  def writeTableData(sourceDF:DataFrame,table:String,mode:SaveMode): Unit ={
    // 写入表
    sourceDF.write.mode(mode).insertInto(table)
  }

  /**
    * 创建SparkSession
    *
    */
  def createSpark(conf:SparkConf):SparkSession={
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    // 加载自定义函数
    registerFun(spark)

    spark
  }

  /**
    * UDF 注册
    */
  def registerFun(spark:SparkSession)={
    //处理年龄段
    spark.udf.register("getAgeRange",QFUdf.getAgeRange _)
  }

  /**
    * 参数校验
    *
    */
  def rangeDates(begin:String,end:String):Seq[String]={
    val bdp_days = new ArrayBuffer[String]()
    try{
      val bdp_date_begin= DateUtil.dateFromat4String(begin,"yyyy-MM-dd")
      val bdp_date_end = DateUtil.dateFromat4String(end,"yyyy-MM-dd")
      // 如果两个时间相等，取其中的第一个开始时间
      // 如果不相等，计算时间差
      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while (cday != bdp_date_end){
          bdp_days.+=(cday)
          // 让初始时间累加，以天为单位
          val pday = DateUtil.dateFromat4StringDiff(cday,1)
          cday = pday
        }
      }
    }catch {
      case ex:Exception=>{
        println("参数不匹配")
        logger.error(ex.getMessage,ex)
      }
    }
    bdp_days
  }

}
