package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object DoubleValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CoalesceTest")
    val context = new SparkContext(conf)
    val rdd1 = context.makeRDD(List(1, 2, 3, 5))
    val rdd2 = context.makeRDD(List(3, 5, 4, 6))
    println(rdd1.intersection(rdd2).collect().mkString(","))
    println(rdd1.union(rdd2).collect().mkString(","))
    println(rdd1.subtract(rdd2).collect().mkString(","))
    println(rdd1.zip(rdd2).collect().mkString(","))
  }
}
