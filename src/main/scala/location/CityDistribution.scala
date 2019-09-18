package location

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CityDistribution {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("inpath not find")
      sys.exit()
    }

    val Array(inputPath) = args

    val spark = SparkSession.builder()
      .appName("citydistribution")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val frame: DataFrame = spark.read.parquet(inputPath)

    frame.show()

    frame.createOrReplaceTempView("t_citydb")

//    spark.sql("select provincename,cityname from t_citydb group by provincename,cityname").createOrReplaceTempView("a")
    spark.sql(
      "select provincename,cityname,"
        +"(case when requestmode=1 and processnode>=1 then 1 else 0 end) processnode1,"
        +"(case when requestmode=1 and processnode>=2 then 1 else 0 end) processnode2,"
        +"(case when requestmode=1 and processnode=3 then 1 else 0 end) processnode3,"
        +"(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) isbid1,"
        +"(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) iswin1,"
        +"(case when requestmode=2 and iseffective=1 then 1 else 0 end) requestmode2,"
        +"(case when requestmode=3 and iseffective=1 then 1 else 0 end) requestmode3,"
        +"(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) winprice,"
        +"(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) adpayment from t_citydb"
    ).createOrReplaceTempView("tc")

    val df = spark.sql(
      "select provincename,cityname," +
        "count(processnode1) processnode1," +
        "count(processnode2) processnode2," +
        "count(processnode3) processnode3," +
        "count(isbid1) isbidcou," +
        "count(iswin1) iswincou," +
        "count(isbid1)/count(iswin1) iswinride, " +
        "count(requestmode2) requestmode2cou," +
        "count(requestmode3) requestmode3cou," +
        "count(requestmode2)/count(requestmode3) requestmode3ride," +
        "sum(winprice) winprice," +
        "sum(adpayment) adpayment from tc group by provincename,cityname order by provincename,cityname"
    )

    //通过config配置文件依赖进行相关配置映射
    val load: Config = ConfigFactory.load("application2.conf")

    //创建Properties对象广播变量
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))

    df.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.table"),prop)

    spark.stop()


  }

}
