package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CoalesceTest")
    val context = new SparkContext(conf)
    val rdd = context.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 2)
    val rdd2 = context.makeRDD(List(("c", 3), ("d", 4), ("e", 5), ("f", 6),("f", 6)), 2)
    rdd.join(rdd2).collect().foreach(println)
    println("###############")
    rdd.leftOuterJoin(rdd2).collect().foreach(println)
    println("###############")
    rdd.rightOuterJoin(rdd2).collect().foreach(println)
    println("###############")
    rdd.cogroup(rdd2).collect().foreach(println)
    println("###############")



  }
}
