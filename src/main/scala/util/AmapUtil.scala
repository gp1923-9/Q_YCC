package util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {
  /**
    * 解析经纬度
    * @param long
    * @param lat
    * @return
    */
  def getBusinessFromAmap(long:Double,lat:Double): String = {
    //https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&key=59283c76b065e4ee401c2b8a4fde8f8b&extensions=all
    val location = long+","+lat
    //获取URL
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=59283c76b065e4ee401c2b8a4fde8f8b&extensions=all"

    //调用Http接口发送请求
    val jsonstr: String = HttpUtil.get(url)

    //解析json串
    val jsonObject: JSONObject = JSON.parseObject(jsonstr)

    //判断当前状态是否为 1
    val status: Int = jsonObject.getIntValue("status")
    if(status == 0) return ""
    //如果不为空
    val jsonObject1 = jsonObject.getJSONObject("regeocode")
    val jsonObject2: JSONObject = jsonObject1.getJSONObject("addressComponent")
    if (jsonObject2.isEmpty) return ""
    val jsonArray: JSONArray = jsonObject2.getJSONArray("businessAreas")
    if(jsonArray.isEmpty) return ""

    //定义集合取值
    val result = collection.mutable.ListBuffer[String]()
    //循环数组
    for (item <- jsonArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        val name: String = json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }
}
