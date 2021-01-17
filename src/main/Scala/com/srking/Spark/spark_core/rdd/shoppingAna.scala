package com.srking.Spark.spark_core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object shoppingAna {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("shoppingAna")
    val sc = new SparkContext(conf)
    val accumulator = new shopAccumulator
    sc.register(accumulator)
    val lines = sc.textFile("FileIn/user_visit_action.txt")
    val strings = top10(lines, accumulator)
    //Top10品类中每个品类的点击Top10
    clickTop10(lines,strings)
  }

  def clickTop10(lines: RDD[String], top10: List[String]): Unit = {
    //把不是点击的数据过滤掉，过滤TOP10中的品类
    val click = lines.filter(action => {
      val strings = action.split("\\_")
      strings(6) != "-1" && top10.contains(strings(6))
    })
    //计算每个品类，每个session的点击量
    val rdd02 = click.map(action => {
      ((action.split("\\_")(6), action.split("\\_")(2)), 1)
    }).reduceByKey(_ + _)
    val result = rdd02.map {
      case ((k, v), cnt) => {
        (k, (v, cnt))
      }
    }
    val groupResult = result.groupByKey()
    val value = groupResult.mapValues(f => {
      val list = f.toList
      list.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })
    value.collect().foreach(println)
  }

  def top10(lines: RDD[String], accumulator: shopAccumulator): List[String] = {
    lines.foreach({ line => {
      val strs = line.split("\\_")
      if (strs(6) != "-1") {
        accumulator.add(("click", strs(6)))
      }
      else if (strs(8) != "null") {
        val str01s = strs(8).split("\\,")
        str01s.map(str => accumulator.add(("order", str)))
      }
      else if (strs(10) != "null") {
        val str01s = strs(10).split("\\,")
        str01s.map(str => accumulator.add(("pay", str)))
      }
    }
    })
    val value = accumulator.value
    val top10 = value.toList.sortWith((f1, f2) => {
      if (f1._2._1 > f2._2._1) {
        true
      }
      else if (f1._2._1 == f2._2._1) {
        if (f1._2._2 > f2._2._2) {
          true
        } else if (f1._2._2 == f2._2._2) {
          f1._2._3 >= f2._2._3
        } else {
          false
        }
      }

      else {
        false
      }

    }
    )
    top10.map(_._1).take(10)
  }


}

class shopAccumulator extends AccumulatorV2[(String, String), collection.mutable.Map[String, (Int, Int, Int)]] {
  private val map01 = collection.mutable.Map[String, (Int, Int, Int)]()

  override def isZero: Boolean = {
    map01.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), collection.mutable.Map[String, (Int, Int, Int)]] = {
    new shopAccumulator
  }

  override def reset(): Unit = {
    map01.clear()

  }

  override def add(v: (String, String)): Unit = {
    val map = v.asInstanceOf[Tuple2[String, String]]
    if (map._1 == "click") {
      val tuple1 = map01.getOrElse(map._2, (0, 0, 0))

      map01.update(map._2, (tuple1._1 + 1, tuple1._2, tuple1._3))

    } else if (map._1 == "order") {
      val tuple1 = map01.getOrElse(map._2, (0, 0, 0))

      map01.update(map._2, (tuple1._1, tuple1._2 + 1, tuple1._3))
    } else {
      val tuple1 = map01.getOrElse(map._2, (0, 0, 0))

      map01.update(map._2, (tuple1._1, tuple1._2, tuple1._3 + 1))
    }
  }

  override def merge(other: AccumulatorV2[(String, String), collection.mutable.Map[String, (Int, Int, Int)]]): Unit = {
    val map02 = other.value.asInstanceOf[mutable.Map[String, (Int, Int, Int)]]
    map02.foreach(t1 => {
      val t2 = map01.getOrElse(t1._1, (0, 0, 0))
      map01.update(t1._1, (t1._2._1 + t2._1, t1._2._2 + t2._2, t1._2._3 + t2._3))
    })
  }

  override def value: mutable.Map[String, (Int, Int, Int)] = map01
}
