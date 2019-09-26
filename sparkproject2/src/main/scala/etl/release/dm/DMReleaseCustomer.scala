package etl.release.dm

import constant.ReleaseConstant
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}
import _root_.util.SparkHelper

import scala.collection.mutable.ArrayBuffer

class DMReleaseCustomer {

}

/**
  * 投放目标客户数据集市
  */
object DMReleaseCustomer {
  //日志
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)
  /**
    * 统计目标客户集市
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String):Unit = {
    //导入内置函数和隐式转换
    //隐式转换：spark2.0 有编码格式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 缓存级别
    val saveMode = SaveMode.Overwrite
    // 内存加磁盘
    val storageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

    //获取日志数据
    val customerColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDWReleaseCustomers()

    //判断 获取当天数据
    val customerCondition = col(s"${ReleaseConstant.DEF_PARTITION}" ) === lit(bdp_day)

    //获取数据
    val customerReleaseDF: Dataset[Row] = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerColumns)
      //条件判断
      .where(customerCondition)
      //缓存级别
      .persist(storageLevel)

    println("DW===========================")

    customerReleaseDF.show(10,false)

    // 统计渠道指标
    val customerSourceGroupColumns = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")

    //插入列
    val customerSourceColumns = DMReleaseColumnsHelper.selectDMCustomerSourceColumns()
    //想要一个组识别里面所有的列要用“:_*”
    val customerSourceDMDF =
    customerReleaseDF.groupBy(customerSourceGroupColumns:_*)
      .agg(countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}"))
    //按照条件查询 --没有则赋值第二个参数给第一个参数再匹配，有则匹配
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      //所有维度列
      .selectExpr(customerSourceColumns:_*)

    //打印
    println("DM ==========================")
    customerSourceDMDF.show(10,false)

    //写入数据
    SparkHelper.writeTableData(customerSourceDMDF,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCE,saveMode)

  }

}
