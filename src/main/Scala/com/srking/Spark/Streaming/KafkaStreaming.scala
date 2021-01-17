package com.srking.Spark.Streaming

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("cp")
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "wcy",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("video_log"), kafkaPara)
    )

//    val value = kafkaStream.map(x => {
//      val strings = x.value().split("\t")
//      strings(2)
//    }).map((_, 1)).updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
//      Option(seq.sum + opt.getOrElse(0))
//    })
//    value.reduceByKey(_ + _).print()
    kafkaStream.foreachRDD(rdd=>{
      rdd.map(cs=>{
        val strings = cs.value().split("\t")
        (strings(2),1)
      }).reduceByKey(_+_).collect().foreach(println)
    })
    //    value.map(arr=>{
    //      (arr(3),1)
    //    }).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
