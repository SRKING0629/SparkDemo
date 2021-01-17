package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Test01")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("FileIn/agent.log")
    //切割数据 省份，广告 ，1
    val rdd01 = lines.map(line => {
      val strings = line.split("\\ ")
      ((strings(4), strings(1)), 1)
    })
    //汇总数据  (省份，广告) ，10
    val rdd02 = rdd01.reduceByKey(_ + _)
    //省份，(广告 ，10)
    val rdd03 = rdd02.map {
      case ((k:String, v:String), v1:Int) => {
        (k, (v, v1))
      }
    }.groupByKey()
    rdd03.mapValues(iter=>{
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }).collect().foreach(println)
    //省份，(广告 ，10) 每个省份取前三
  }
}
