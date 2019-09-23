package Tags

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import util.TagsUtils

/**
  * 上下文标签主类
  */
object TagsContext2 {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath, docs, stopwords) = args
    //    val Array(inputPath) = args

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    //    // 调用HbaseAPI
    //    val load = ConfigFactory.load("application2.conf")
    //    // 获取表名
    //    val HbaseTableName = load.getString("HBASE.tableName")
    //    // 创建Hadoop任务
    //    val configuration = spark.sparkContext.hadoopConfiguration
    //    // 配置Hbase连接
    //    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    //    // 获取connection连接
    //    val hbConn = ConnectionFactory.createConnection(configuration)
    //    val hbadmin = hbConn.getAdmin
    //    // 判断当前表是否被使用
    //    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
    //      println("当前表可用")
    //      // 创建表对象
    //      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
    //      // 创建列簇
    //      val hColumnDescriptor = new HColumnDescriptor("tags")
    //      // 将创建好的列簇加入表中
    //      tableDescriptor.addFamily(hColumnDescriptor)
    //      hbadmin.createTable(tableDescriptor)
    //      hbadmin.close()
    //      hbConn.close()
    //    }
    //    val conf = new JobConf(configuration)
    //    // 指定输出类型
    //    conf.setOutputFormat(classOf[TableOutputFormat])
    //    // 指定输出哪张表
    //    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)

    // 读取数据文件
    val df = spark.read.parquet(inputPath)

    // 读取字典文件
    val docsRDD = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()
    // 广播字典
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    // 读取停用词典
    val stopwordsRDD = spark.sparkContext.textFile(stopwords).map((_, 0)).collectAsMap()
    // 广播字典
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)

    val allUserId = df.rdd.map(row => {
      //获取所有ID
      val strList: List[String] = TagsUtils.getallUserId(row)
      (strList, row)
    })
//    allUserId.foreach(println)

    //构建点集合
    val verties = allUserId.flatMap(row => {
      //获取所有数据
      val rows = row._2
      val adList: List[(String, Int)] = TagsAd.makeTags(rows)
      //获取商圈
      //      val businessList = BusinessTag.makeTags(row)
      // 媒体标签
      val appList = TagsAPP.makeTags(rows, broadValue)
      // 设备标签
      val devList = TagsDevice.makeTags(rows)
      // 地域标签
      val locList = TagsLocation.makeTags(rows)
      // 关键字标签
      val kwList = TagsKword.makeTags(rows, broadValues)
      //获取所有标签
      // val tagList: List[(String, Int)] = adList++appList++businessList++devList++locList++kwList
      val tagList: List[(String, Int)] = adList++appList++devList++locList++kwList
//      tagList
//      保留用户ID
      val VD: List[(String, Int)] = row._1.map((_, 0))++tagList
      /*
       * 思考 1.如何保证其中一个ID携带着用户的标签
       *      2.用户ID的字符串如何处理
       */
      row._1.map(uId => {
        if (row._1.head.equals(uId)) {
          (uId.hashCode.toLong,VD)
        } else {
          (uId.hashCode.toLong,List.empty)
        }
      })
    })

    //构建边的集合
    val edges = allUserId.flatMap(row => {
      //A B C: A->B A->C
      row._1.map(uId => {
        Edge(row._1.head.hashCode.toLong, uId.hashCode().toLong, 0)
      })
    })
    //构件图
       val graph = Graph(verties, edges)
    //根据图计算中的连通算法，通过途中的分支，连通所有的点
    //然后根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    //聚合所有的标签
//    vertices.join(verties)
//      .map {
//        case (uId, (cnId, tagsAndUserId)) => {
//          (cnId, tagsAndUserId)
//        }
//      }
//      //聚合value --聚合所有标签
//      .reduceByKey((list1, list2) => {
//      (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).size).toList
//    }).foreach(println)

    // 聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.filter(_._2.isEmpty==false).foreach(println)

  }
}
