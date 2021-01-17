package com.srking.Spark.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WorkCount")
    val context = new SparkContext(conf)
    val fileContext: RDD[String] = context.textFile("FileIn/a_10014_14001_20210112_00_001.dat")
    fileContext.collect().foreach(println)
//    fileContext.flatMap(_.split("\\ ")).map((_, 1)).groupByKey().map({
//      case (word, list) => {
//        (word, list.reduce({
//          (t1, t2) => {
//            t1 + t2
//          }
//        }))
//      }
//    }).collect().foreach(println(_))
    context.setLogLevel("error")
    fileContext.flatMap(_.split("\\ ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println(_))
    //    val value: RDD[String] = fileContext.flatMap(_.split("\\ "))
    //    val value1: RDD[(String, Iterable[String])] = value.groupBy(key => key)
    //    value1.collect().foreach(println(_))
    //     value1.map({
    //       case (word,list)=>(word,list.size)
    //       case _=>println("我爱你")
    //     }).collect().foreach(println(_))
  }
}
