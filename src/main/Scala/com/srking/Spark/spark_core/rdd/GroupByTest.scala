package com.srking.Spark.spark_core.rdd

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object GroupByTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByTest").setMaster("local[*]")
    val context = new SparkContext(conf)
    val value = context.textFile("FileIn/groupby.txt")
    value.map(str => {
      val date = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(str)
      (new SimpleDateFormat("HH").format(date),1)
    }).groupBy(_._1).map(str=>{
      (str._1,str._2.size)
    }).collect().foreach(println)
  }
}
