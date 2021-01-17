package com.srking.Spark.spark_core.rdd

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object accumulater {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddCreate")
    val context = new SparkContext(conf)
    val ints = List("a", "b", "v", "a", "d")
    val lines = context.makeRDD(ints, 5)
    val myaccumulator = new Myaccumulator
    context.register(myaccumulator)
    val value = lines.map(num => {
      myaccumulator.add(num)
    })
    value.collect()
    println(myaccumulator.value)
  }
}

class Myaccumulator extends AccumulatorV2[String, collection.mutable.Map[String, Int]] {
  private val map = collection.mutable.Map[String, Int]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val myaccumulator = new Myaccumulator
    myaccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map.update(v, map.getOrElse(v, 0) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map01 = this.map
    val map02 = other.value
    map02.foreach({
      case (str, cnt) => {
        map01.update(
          str, map01.getOrElse(str, 0) + 1)
      }
    })
  }

  override def value: mutable.Map[String, Int] = {

    map
  }
}