package com.srking.Spark.mgh

import java.text.SimpleDateFormat

import com.srking.Spark.utils.{HBaseUtil, RedisClusterConnUtil, RedisClusterMethodUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.xml.XML

case class video_log(date_id: String, user_id: String, video_id: String)

object NewUsersPVUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NewUsersPVUV").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(3))
    val elem = XML.loadFile("src/main/Scala/com/srking/Spark/utils/mgh.xml")
    val group_id = (elem \\ "group_id").text
    val topic = (elem \\ "topic").text

    val tableName = (elem \\ "tableName").text
    val columnFamily = (elem \\ "columnFamily").text
    val column = (elem \\ "column").text


    val kafkaparams = Map[String, String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer")
    //TODO 读取偏移量 redis: key - (k,v)  (group_id)->(topic,partition-> offset)
    val cols = RedisClusterMethodUtil.getCols(group_id)
    val kafka_partition_off = collection.mutable.Map[TopicPartition, Long]()
    for (col <- cols) {
      //opic,partition
      val top_partition = col._1
      //offset
      val offset = col._2.toString.toLong
      val topic_name = top_partition.split("\\^")(0)
      val partition = top_partition.split("\\^")(1).toInt

      kafka_partition_off.put(new TopicPartition(topic_name, partition), offset)
    }
    println("---已初始化")
    var ds: InputDStream[ConsumerRecord[String, String]] = null
    if (kafka_partition_off.isEmpty) {
      ds = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaparams)

      )
    } else {
      ds = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaparams, kafka_partition_off)
      )
    }
    println("----已连接")
    val redis = RedisClusterConnUtil.getRedisConn()
    ds.foreachRDD(rdd => {
      val rdd01 = rdd.map(cs => {
        val words = cs.value().split("\t")
        //2020-11-11 23:00:20
        val dateformat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
        val date = dateformat.parse(words(0))
        val date_id = dateformat.format(date, "yyyymmdd")

        video_log(date_id, words(2), words(8))
      })
      rdd01.map(vl => {
        if (hbaseExits(vl.user_id)) {
          redis.sadd("clyh",vl.user_id)
        }
        else {
          redis.sadd("xzyh",vl.user_id)
        }
      })
      println(redis.scard("clyh"))
      println(redis.scard("xzyh"))
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def hbaseExits(user_id: String): Boolean = {
    val HBconnection = HBaseUtil.getConnection()
    val elem = XML.loadFile("src/main/Scala/com/srking/Spark/utils/mgh.xml")
    val tableName = (elem \\ "tableName").text
    val table = HBconnection.getTable(TableName.valueOf(tableName))
    table.exists(new Get(Bytes.toBytes(user_id)))
  }
}
