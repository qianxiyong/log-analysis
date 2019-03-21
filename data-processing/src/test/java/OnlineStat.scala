import java.util.Date

import com.atguigu.utils._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object OnlineStat {

  def main(args: Array[String]): Unit = {
    val checkPointPath = PropertiesUtils.loadProperties("streaming.checkpoint.path")
    val streamingContext = StreamingContext.getActiveOrCreate(checkPointPath, createFunc())

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createFunc(): () => _root_.org.apache.spark.streaming.StreamingContext = {
    () => {
      // 第一步： 创建streamingContext
      // 创建sparkConf
      val sparkConf = new SparkConf().setAppName("online").setMaster("local[*]")
      // 指定任务优雅地停止
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 消费kafka的最大速率
      sparkConf.set("spark.streaming.maxRatePerPartition", "100")
      // 指定序列化方式是Kryo
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator")

      // 创建streamingContext
      val interval = PropertiesUtils.loadProperties("streaming.interval").toLong
      val streamingContext = new StreamingContext(sparkConf, Seconds(interval))

      // 启动streamingContext的checkPoint机制
      val checkPointPath = PropertiesUtils.loadProperties("streaming.checkpoint.path")
      streamingContext.checkpoint(checkPointPath)

      // 第二步：获取Kafka配置信息
      val kafkaBrokers = PropertiesUtils.loadProperties("kafka.broker.list")
      val kafkaTopic = PropertiesUtils.loadProperties("kafka.topic")
      val kafkaTopicSet:Set[String] = Set(kafkaTopic)
      val kafkaGroup = PropertiesUtils.loadProperties("kafka.groupId")

      val kafkaParam = Map(
        "bootstrap.servers" -> kafkaBrokers,
        "group.id" -> kafkaGroup
      )

      // 第三步：获取过去已经存在Zookeeper里面的offset信息
      val kafkaCluster = new KafkaCluster(kafkaParam)
      val topicAndPartitionOffsetInfo = ZookeeperUtils.getOffsetFromZookeeper(kafkaCluster, kafkaGroup, kafkaTopicSet)

      // 第四步：创建DStream
      // onlineDStream：DStream[RDD[log:String(Json)]]
      val onlineDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](streamingContext, kafkaParam, topicAndPartitionOffsetInfo,
        (mess:MessageAndMetadata[String, String]) => mess.message()
      )

      onlineDStream.checkpoint(Duration(10000))

      // 第五步：对DStream进行处理，将统计结果写入HBase
      // onlineDStream：DStream[RDD[log:String(Json)]]
      val onlineFilterDStream = onlineDStream.filter{
        case log =>
          var success = true

          if(!log.contains("appVersion") && !log.contains("errorMajor") && !log.contains("currentPage")){
            success = false
          }

          if(log.contains("appVersion")){
            val startupLog = JsonUtils.json2StartupLog(log)
            if(startupLog == null || startupLog.getCity == null || startupLog.getUserId == null || startupLog.getAppId == null){
              success = false
            }
          }

          success
      }

      // 实时统计每一天每个城市实时点击次数
      onlineFilterDStream.foreachRDD{
        rdd =>
          rdd.foreachPartition{
            items =>
              val table = HBaseUtils.getHBaseTabel(PropertiesUtils.getProperties())
              for(item <- items){
                val startupLog = JsonUtils.json2StartupLog(item)
                val city = startupLog.getCity
                val date = new Date(startupLog.getStartTimeInMs)
                // dateTime : yyyy-MM-dd
                val dateTime = DateUtils.dateToString(date)
                val rowkey = dateTime + "_" + city
                table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes("StatisticData"), Bytes.toBytes("clickCount"), 1L)
              }
          }
      }

      // 第七步：将最新的offset写入Zookeeper
      ZookeeperUtils.offsetToZookeeper(onlineDStream, kafkaCluster, kafkaGroup)

      streamingContext
    }
  }
}
