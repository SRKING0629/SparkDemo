package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object SortByTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val context = new SparkContext(conf)
    val ints = Array(32,3131,66,7757,34555)
    val lines = context.makeRDD(ints,5)

    lines.sortBy(num=>num,false).collect().foreach(println)
    context.stop()
//    lines.collect().foreach(println)
  }
}
