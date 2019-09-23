package Test.W1

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/**
  *  1、按照pois，分类businessarea，并统计每个businessarea的总数。
  */
object T1 {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录有误")
      sys.exit()
    }

    val Array(inputPath) = args

    val spark = SparkSession.builder().master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val log: Dataset[String] = spark.read.textFile(inputPath)

    val logs: mutable.Buffer[String] = log.collect().toBuffer

    var list: List[List[String]] = List()

    for(i <- 0 until logs.length){
      val str: String = logs(i).toString

      //json解析
      val jsonparse: JSONObject = JSON.parseObject(str)

      //判断当前状态是否为 1
      val status = jsonparse.getIntValue("status")
      if(status == 0) return ""

      //不为空则
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null || poisArray.isEmpty) return null

      //创建集合作为存储工具
      val result = collection.mutable.ListBuffer[String]()
      //循环输出，并存储到集合中
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          result.append(json.getString("businessarea"))
        }
      }
      val list1: List[String] = result.toList

      list:+=list1
    }

    val res1: List[(String, Int)] = list.flatMap(x => x)
      .filter(x => x != "[]").map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size)
      .toList.sortBy(_._2).reverse

    res1.foreach(println)

  }
}
