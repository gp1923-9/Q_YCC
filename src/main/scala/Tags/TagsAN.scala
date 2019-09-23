package Tags

import org.apache.spark.sql.{Row}
import util.Tag


object TagsAN extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String, Int)]()
    //获取数据类型
    val row: Row = args(0).asInstanceOf[Row]
    //获取appname类型
//    val adType: String = row.getAs[String]("appname")
//appname名称
    val adName = row.getAs[String]("appname")
    //appname标签
    adName match {
      case v  => list:+=("APP "+v,1)
    }
    list
  }
}
