package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ActionRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val context = new SparkContext(conf)
    val ints = Array(1, 2, 3, 4)
    val lines = context.makeRDD(ints,5)
    println(lines.reduce(_ + _))
    println(lines.countByValue())
  }
}
