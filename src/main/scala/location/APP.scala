package location

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RptUtils

/**
  * 媒体分析指标
  */
/*
 * 类和对象的区别：
 * 类 -> 实例化，创建，会GC
 * object创建的是常量，静态方法，不会小GC会FULL GC
 */
class APP{
}
object APP {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("inpath not find")
      sys.exit()
    }

    val Array(inputPath,outputPath,docs) = args

    val spark = SparkSession.builder()
      .appName("citydistribution")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("Error")

    //读取数据字典
    val docMap: collection.Map[String, String] = sc.textFile(docs).map(_.split("\\s", -1))
      .filter(_.length >= 5)
      //key=appid value=appname
      .map(arr => (arr(4), arr(1)))
      //直接将show和map连接在一起了
      //collect+map
      .collectAsMap()

    //进行广播
    val broadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(docMap)

    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)
    df.rdd.map(x=>{
      //获取媒体字段
      var appName = x.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broadcast.value.getOrElse(x.getAs[String]("appid"),"unknow")
      }

      val requestmode: Int = x.getAs[Int]("requestmode")
      val processnode = x.getAs[Int]("processnode")
      val iseffective = x.getAs[Int]("iseffective")
      val isbilling = x.getAs[Int]("isbilling")
      val isbid = x.getAs[Int]("isbid")
      val iswin = x.getAs[Int]("iswin")
      val adorderid = x.getAs[Int]("adorderid")
      val winprice = x.getAs[Double]("winprice")
      val adpayment = x.getAs[Double]("adpayment")

      //处理请求数
      val reqList: List[Double] = RptUtils.requestPt(requestmode, processnode)
      //处理点击展示数
      val clickList: List[Double] = RptUtils.clickPt(requestmode,iseffective)
      //处理参与竞价数和成功数
      val adList: List[Double] = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有指标做累加
      val allList: List[Double] = reqList ++ clickList ++ adList

      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      //拉链
      //list(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      val list3: List[Double] = list1.zip(list2).map(t=>t._1+t._2)
      list3
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)

  }
}
