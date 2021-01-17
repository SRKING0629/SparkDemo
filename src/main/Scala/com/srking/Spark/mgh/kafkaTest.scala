package com.srking.Spark.mgh

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.Source

object kafkaTest {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer");
    val kp = new KafkaProducer[String, String](properties)

    val vidoe = Source.fromFile("FileIn/20201112_1.txt")
    val lines = vidoe.getLines()
    while (lines.hasNext) {
      val line = lines.next()
      kp.send(new ProducerRecord[String, String]("video_log", line, line))
      println(line)
      Thread.sleep(1000)
    }
    kp.close()
  }
}
