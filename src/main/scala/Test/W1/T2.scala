package Test.W1

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/**
  *  2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
  */

object T2 {
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

    var list: List[String] = List()
    val log: Dataset[String] = spark.read.textFile(inputPath)

    val logs: mutable.Buffer[String] = log.collect().toBuffer

    for(i <- 0 until logs.length) {
      val str: String = logs(i).toString

      val jsonparse: JSONObject = JSON.parseObject(str)

      //判断当前状态是否为 1
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""

      //为1则
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return null

      //定义集合取值
      val result = collection.mutable.ListBuffer[String]()
      //循环输出，并存储到集合中
      for (item <- poisArray.toArray) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          result.append(json.getString("type"))
        }
      }

      list:+=result.mkString(";")
    }

    val res2: List[(String, Int)] = list.flatMap(x => x.split(";"))
      //为每一个Type类型打上标签
      .map(x => ("type：" + x, 1))
      // 统计各标签的数量
      .groupBy(x => x._1)
      .mapValues(x => x.size)
      //排序
      .toList.sortBy(x => x._2)

    res2.foreach(println)

  }
}
