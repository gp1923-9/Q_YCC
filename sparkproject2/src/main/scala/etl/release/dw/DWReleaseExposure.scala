package etl.release.dw

import com.qf.bigdata.release.enums.ReleaseStatusEnum
import constant.ReleaseConstant
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import util.SparkHelper

import scala.collection.mutable.ArrayBuffer

/**
  * DW 曝光主题
  */
object DWReleaseExposure {
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
      /*
       * 如何选择存储级别
       * Spark 的存储级别的选择，核心问题是在内存使用率和 CPU 效率之间进行权衡。建议按下面的过程进行存储级别的选择 :
       * 如果使用 MEMORY_ONLY 存储在内存中的 RDD / DataFrame 没有发生溢出，那么就选择默认的存储级别。默认存储级别可以最大程度的提高 CPU 的效率,可以使在 RDD / DataFrame 上的操作以最快的速度运行。
       * 如果内存不能全部存储 RDD / DataFrame ，那么使用 MEMORY_ONLY_SER，并挑选一个快速序列化库将对象序列化，以节省内存空间。使用这种存储级别，计算速度仍然很快。
       * 除了在计算该数据集的代价特别高，或者在需要过滤大量数据的情况下，尽量不要将溢出的数据存储到磁盘。因为，重新计算这个数据分区的耗时与从磁盘读取这些数据的耗时差不多。
       * 如果想快速还原故障，建议使用多副本存储级别 MEMORY_ONLY_2 / MEMORY_ONLY_SER_2 。所有的存储级别都通过重新计算丢失的数据的方式，提供了完全容错机制。但是多副本级别在发生数据丢失时，不需要重新计算对应的数据库，可以让任务继续运行。
       */
      val storagelevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

      //设置存储模式
      /*
       * overwrite/comperate/append
       */
      val saveMode: SaveMode = SaveMode.Overwrite

      //获取日志字段数据
      val exposureColumns: ArrayBuffer[String] = DWReleaseColumnsHelper.selectDWReleaseExposureColumns()

      //设置条件，当天数据，获取目标客户：03
      val exposureReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        and
        col(s"${ReleaseConstant.COL_RLEASE_SESSION_STATUS}")
        === lit(ReleaseStatusEnum.SHOW.getCode)
        )

      //ODS层读的表
      val exposureReleaseDF = SparkHelper.readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,exposureColumns)
        // 填入条件
        .where(exposureReleaseCondition)
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
      println("DWReleaseDF=====================================")
      // 打印查看结果
      exposureReleaseDF.show(10,false)
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
