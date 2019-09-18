package location

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 统计省市指标
  */
object ProCityCt {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("inpath not find")
      sys.exit()
    }
    val Array(inputPath) = args

    val spark = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")


    //获取数据
    //inputPath = F:\sparkProject\ouput\parquet
    val df: DataFrame = spark.read.parquet(inputPath)
    //注册临时视图
    df.createOrReplaceTempView("log")
    df.show()
    //执行sparksql
    val df2 = spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //存本地
    df2.write.partitionBy("provincename","cityname").json("F:\\sparkProject\\ouput\\json")
    //存Mysql

    //通过config配置文件依赖进行相关配置映射
    val load: Config = ConfigFactory.load()

    //创建Properties对象广播变量
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))

    df2.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.table"),prop)

    spark.stop()
  }
}
