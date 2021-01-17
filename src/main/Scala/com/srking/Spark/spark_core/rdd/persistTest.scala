package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object persistTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val context = new SparkContext(conf)
    context.setCheckpointDir("copy")
    val ints = Array(1, 2, 3, 4)
    val lines = context.makeRDD(ints,5)
    lines.checkpoint()
    lines.collect().foreach(println)
  }
}
