package util

/**
  * 处理指标工具类
  */
object RptUtils {
  //处理请求数
  def requestPt(requestmode: Int,processnode:Int):List[Double]={
    if(requestmode == 1 && processnode == 1){
      //(1,0,0)第一个是原始请求，第二个是有效请求，第三个是广告请求
      List[Double](1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List[Double](1,1,0)
    }else if(requestmode == 1 && processnode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  //处理点击展示数
  def clickPt(requestmode:Int,iseffective:Int):List[Double]={
    if (requestmode == 2 && iseffective ==1 ){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective ==1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }
  //处理参与竞价数和成功数
  def adPt(iseffective:Int,isbilling:Int,isbid:Int,
           iswin:Int,adorderid:Int,winprice:Double,
           adpayment:Double):List[Double]={
    if(iseffective==1 && isbilling == 1 && isbid == 1){
      if(iseffective==1 && isbilling == 1 && iswin == 1&& adorderid != 0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
}
