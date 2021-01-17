package com.srking.Spark.spark_core.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


case class tmp1(date: String, session_id: String)

object Test01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test01").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val rdd01 = ss.sparkContext.textFile("FileIn/user_visit_action.txt")
    val rdd02 = rdd01.flatMap(line => {
      val strings = line.split("\\_")
      List((strings(0), strings(1)))
    })
    import ss.implicits._
    // TODO rdd-dataframe
    val df = rdd02.toDF("date", "session_id")
    // TODO rdd-dataSet
    val ds = rdd02.map(a => {
     tmp1(a._1,a._2)
    }).toDS()
//    ds.show()

    val rdd1 = df.rdd
//    rdd1.foreach(println)
    val tmp = df.createOrReplaceTempView("tmps")
//    ss.sql("select * from tmps limit 5").show()
    val tmp2 = ds.createOrReplaceTempView("tmp2")
    ss.sql("select * from tmp2 limit 5").show()
  }
}
