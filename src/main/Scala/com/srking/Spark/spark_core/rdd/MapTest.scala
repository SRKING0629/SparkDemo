package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}
object MapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val context = new SparkContext(conf)
    val value = context.makeRDD(List(1, 2, 3, 4),2)
    val value1 = value.mapPartitions(iter => {
      List(iter.max).iterator
    })
    value.mapPartitionsWithIndex((index,iter)=>{
      if(index==1){
        iter
      }else{
        Nil.iterator
      }
    }).collect().foreach(println)
//    value1.collect().foreach(println)
    context.stop()
  }
}
