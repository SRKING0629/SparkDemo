package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object AggregateTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CoalesceTest")
    val context = new SparkContext(conf)
    val value = context.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 1), ("b", 2), ("b", 3), ("b", 4)), 2)
    //      val value1 = value.aggregateByKey((0, 0))(
    //        (t, v) => {
    //          (t._1 + 1, t._2 + v)
    //        },
    //        (t, v) => {
    //          (t._1 + v._1, t._2 + v._2)
    //        }
    //      )
    //      value1.mapValues (a=>{
    //        a._2/a._1
    //      }).collect().foreach(println)
    value.combineByKey(
      v => (v, 1),
      (t1: (Int, Int), t2) => {
        (t1._1 + t2, t1._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t1._1, t1._2 + t2._2)
      }
    ).map(p => {
      (p._1, p._2._1 / p._2._2)
    }
    ).collect().foreach(println)
  }
}
