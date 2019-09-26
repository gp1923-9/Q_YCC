package etl.release.dm

import com.qf.bigdata.release.enums.ReleaseStatusEnum
import constant.ReleaseConstant
import etl.release.dw.{DWReleaseColumnsHelper, DWReleaseExposure}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import util.SparkHelper

import scala.collection.mutable.ArrayBuffer

/**
  * DM 目标客户及时库表
  * 渠道用户统计
  */
object DMCustomerSources {
  //日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseExposure.getClass)

  /**
    * 曝光主题
    * release_status="03"
    * @param args
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String):Unit = {
    //获取当前时间
    val begin: Long = System.currentTimeMillis()
    try{
      //隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //设置缓存级别
      val storagelevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

      //设置存储模式
      /*
       * overwrite/comperate/append
       */
      val saveMode: SaveMode = SaveMode.Overwrite

      //获取日志字段数据
      val customerSourcesColumns: ArrayBuffer[String] = DWReleaseColumnsHelper.selectDMCustomerSourcesColumns()

      //设置条件，当天数据，获取目标客户：01
      val exposureReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
//        and
//        col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")
//          === lit(ReleaseStatusEnum.SHOW.getCode)
        )

      //DW层读的表
      val exposureReleaseDF: DataFrame = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerSourcesColumns)

      println("DWReleaseDF=====================================")

      //进行sql API操作
      exposureReleaseDF
        .where(exposureReleaseCondition)
        .groupBy("sources", "channels", "device_type")
        .agg(count("release_status") as "user_count",
          count("release_session") as "total_count")
        .selectExpr(DMReleaseColumnsHelper.selectDMCustomerSourceColumns():_*)
        // 重分区
//        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
        // 打印查看结果
        .show(10,false)


      // 目标用户（存储）
      //      SparkHelper.writeTableData(customerReleaseDF,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

    }catch {
      // 错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      // 任务处理的时长
      s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}"
    }
  }

  /**
    * 投放目标用户
    */
  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String): Unit ={
    var spark:SparkSession =null
    try{
      // 配置Spark参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")
      // 创建上下文
      spark = SparkHelper.createSpark(conf)
      // 解析参数
      val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      // 循环参数
      for(bdp_day <- timeRange){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "dw_release_job"
    val bdp_day_begin = "2019-09-24"
    val bdp_day_end = "2019-09-24"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }
}
