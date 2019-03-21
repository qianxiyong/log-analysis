package com.atguigu.online

import java.util.{Date, Properties}

import com.atguigu.model.StartupReportLogs
import com.atguigu.utils.{DateUtils, HBaseUtils, JsonUtils}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

object KafkaToHBase {

  def main(args: Array[String]): Unit = {
    val checkPath = "/zk/kafkaToHBase1";
    val context: StreamingContext = StreamingContext.getActiveOrCreate(checkPath, creatingFunc)
    context.start()
    context.awaitTermination()
  }

  def getOffset(kafkaCluster: KafkaCluster, groupid: String, topic: String): Map[_root_.kafka.common.TopicAndPartition, Long] = {
    var partitionToLong = new HashMap[TopicAndPartition, Long]();
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
    if (topicAndPartitions.isRight) {
      val topicAndPartition: Set[TopicAndPartition] = topicAndPartitions.right.get
      val topicAndPartitionWithLong: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupid, topicAndPartition)
      if (topicAndPartitionWithLong.isRight) {
        val partitionToLongOld: Map[TopicAndPartition, Long] = topicAndPartitionWithLong.right.get
        partitionToLong ++= partitionToLongOld
        for (s <- partitionToLong) {
          println("++++++++++++++++++=" + s._1 + " " + s._2)
        }
      } else {
        for (tmp <- topicAndPartition) {
          println("++++++++++++++++++=")
          partitionToLong += ((tmp, 0L));
        }
      }
    }
    return partitionToLong;
  }

  def setOffSet(groupid: String, kafkaCluster: KafkaCluster, filterStream: DStream[String]) = {
    filterStream.foreachRDD(rdd => {
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (range <- ranges) {
        val topicAndPartition: TopicAndPartition = range.topicAndPartition()
        val untilOffset: Long = range.untilOffset
        val ack: Either[Err, Map[TopicAndPartition, Short]] = kafkaCluster.setConsumerOffsets(groupid, Map[TopicAndPartition, Long](topicAndPartition -> untilOffset))
        if (ack.isLeft) {
          println(s"Error:${ack.left.get}")
        } else {
          println(s"Success:${topicAndPartition.partition}:${untilOffset}")
        }
      }
    })
  }

  def creatingFunc(): StreamingContext = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaToHBase")
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 配置Spark Streaming查询1s从kafka一个分区消费的最大的message数量
    sparkConf.set("spark.streaming.maxRatePerPartition", "100")

    // 配置Kryo序列化方式
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator")
    streamingContext.checkpoint("/zk/kafkaToHBase1")

    val topic = "log-analysis"
    val groupid = "g1"

    // kafka参数封装
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop101:9092",
      "group.id" -> groupid
    )

    val kafkaCluster = new KafkaCluster(kafkaPara);
    val partitionToLong: Map[TopicAndPartition, Long] = getOffset(kafkaCluster, groupid, topic)
    println(partitionToLong.toString())
    val dStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      streamingContext,
      kafkaPara,
      partitionToLong,
      (mess: MessageAndMetadata[String, String]) => mess.message()
    )
    //val filterStream: DStream[String] = dStream.filter(_.contains("city"))
    dStream.foreachRDD(rdd => {
      rdd.foreach(str => {
        val logs: StartupReportLogs = JsonUtils.json2StartupLog(str)
        val city = logs.getCity
        println("==============" + city)
        val time = DateUtils.dateToString(new Date(logs.getStartTimeInMs))
        val rowKey = city + "_" + time
        val table: Table = HBaseUtils.getHBaseTabel(new Properties())
        table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("info"), Bytes.toBytes("count"), 1L)
        table.close()
      })
    })
    setOffSet(groupid, kafkaCluster, dStream)
    streamingContext
  }
}
