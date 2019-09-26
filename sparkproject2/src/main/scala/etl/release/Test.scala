package etl.release

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
//      .config("spark.warehouse","hdfs://hadoop0001:9000/usr/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from gp23_ods.ods_01_release_session limit 10").show()
    spark.close()
  }
}
