package com.srking.Spark.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test01 {
  def main(args: Array[String]): Unit = {
    //TODO  创建SparkStreamContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc = new StreamingContext(conf, Seconds(3))
    //监控端口数据
    val lines = ssc.socketTextStream("localhost", 6666, StorageLevel.MEMORY_ONLY)
    val word = lines.flatMap(_.split("\\,"))
    val value = word.map((_, 1)).reduceByKey(_ + _)
    value.print()
    ssc.start()
    ssc.awaitTermination()
//    ssc.stop()
  }
}
