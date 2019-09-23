package location

import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RptUtils

/**
  * -DHADOOP_HOME_NAME=$HADOOP_HOME
  * inputPath = F:\sparkProject\ouput\parquet
  * outputPath = F:\sparkProject\ouput\02
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("inpath not find")
      sys.exit()
    }

    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder()
      .appName("citydistribution")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val frame: DataFrame = spark.read.parquet(inputPath)

//    import spark.implicits._
//    frame.map(x=>{
//      val requestmode: Int = x.getAs[Int]("requestmode")
//      val processnode = x.getAs[Int]("processnode")
//      val iseffective = x.getAs[Int]("iseffective")
//      val isbilling = x.getAs[Int]("isbilling")
//      val isbid = x.getAs[Int]("isbid")
//      val iswin = x.getAs[Int]("iswin")
//      val adorderid = x.getAs[Int]("adorderid")
//      val winprice = x.getAs[Double]("winprice")
//      val adpayment = x.getAs[Double]("adpayment")
//    })

    frame.rdd.map(x=>{
      //根据指标字段获取数据
      //REQUESTMODE PROCESSNODE ISEFFECTIVE ISBILLING ISBID ISWIN ADORDERID
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

      //mapPartition foreachPartition
      /*使用场景: 使用中小型数据量级 List容易出现OOM
       * 效率提升，
       * hashcode 做hashpartitioner的时候出现数据倾斜，task资源有限
       */
      ((x.getAs[String]("provincename"),x.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      //拉链
      //list(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      val list3: List[Double] = list1.zip(list2).map(t=>t._1+t._2)
      list3
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)
  }
}
