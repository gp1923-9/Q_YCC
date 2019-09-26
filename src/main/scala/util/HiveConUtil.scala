package util

package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 新版本spark-hive测试
  */
object HiveConUtil {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()
  }
}