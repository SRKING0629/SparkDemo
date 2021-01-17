package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CoalesceTest {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("CoalesceTest")
      val context = new SparkContext(conf)
      val value = context.makeRDD(List(1,2,3,4),4)
      val value1 = value.coalesce(2, true)
      value1.saveAsTextFile("out")
      //    value1.collect().foreach(println)
      context.stop()
    }
}
