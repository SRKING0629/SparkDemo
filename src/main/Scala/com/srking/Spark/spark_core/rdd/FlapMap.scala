package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object FlapMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val context = new SparkContext(conf)
    val value = context.makeRDD(List(List(1,2,3,4),3,List(4,5,6,7)),2)
     value.flatMap(data=>{
       data match {
         case list:List[_]=>list
         case data1=>List(data1)
       }
     }).collect().foreach(println)
    //    value1.collect().foreach(println)
    context.stop()
  }
}
