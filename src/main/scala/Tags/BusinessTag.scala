package Tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import util.{AmapUtil, JedisConnectionPool, String2Type, Tag}

/**
  * 商圈标签
  */
object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //获取数据
    val row = args(0).asInstanceOf[Row]
    //获取经纬度
    if(String2Type.toDouble(row.getAs[String]("long")) >= 73
    && String2Type.toDouble(row.getAs[String]("long")) <= 136
    && String2Type.toDouble(row.getAs[String]("lat"))>= 3
    && String2Type.toDouble(row.getAs[String]("lat"))<= 53){
      //经纬度
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
//      getBusiness(long,lat)
      //获取到商圈名称
      val business = getBusiness(long,lat)
      if(StringUtils.isNoneBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list:+=(str,1)
        })
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double):String={
    //GeoHash
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //数据库查询当前的商圈信息
    var business = redis_queryBusiness(geohash)
    //如果没有的话去高德
    if(business==null){
      business = AmapUtil.getBusinessFromAmap(long,lat)
      if(business != null && business.length>0){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }

  /**
    * 查询商圈数据库
    */
  def redis_queryBusiness(geohash: String):String={
    val jedis = JedisConnectionPool.getConnection()
    val busniess = jedis.get(geohash)
    jedis.close()
    busniess
  }

  /**
    * 将商圈保存数据库
    */
  def redis_insertBusiness(geohash: String,businese:String): Unit ={
    val jedis = JedisConnectionPool.getConnection()
    val busniess = jedis.set(geohash,businese)
    jedis.close()
  }

}
