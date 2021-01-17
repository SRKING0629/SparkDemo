package com.srking.Spark.spark_core.rdd

import java.text.SimpleDateFormat

import com.srking.Spark.spark_core.rdd.shoppingAna.{clickTop10, top10}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class userPage(uid: String, session_id: String, page_id: String, time: String)

object PageTransform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("shoppingAna")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("FileIn/user_visit_action.txt")
    val tjList = List(1, 2, 3, 4, 5, 6, 7)
    val reList = tjList.zip(tjList.tail)
    //格式化数据 一个用户在一个session页面的转化
    //(user_id,session_id ,(page_id,page_id))
    val rdd01: RDD[userPage] = lines.map(action => {
      val strings = action.split("\\_")
      userPage(strings(1), strings(2), strings(3), strings(4))
    }
    )
    val rdd02 = rdd01.groupBy(iter => {
      iter.session_id
    }).map(iter => {
      iter._2.toList.sortBy(iter => {
        iter.time
      })
    })
    val rdd03 = rdd02.map(list => {
      val strings = list.map(_.page_id.toLong)
      val tuples = strings.zip(strings.tail)
      tuples
    })

    val rdd04 = rdd03.flatMap(iter => iter).map(t => {
      (t, 1)
    })

    val rdd05 = rdd04.filter(iter => {
      reList.contains(iter._1)
    })

    val rdd06 = rdd05.reduceByKey(_ + _)
    //每个页面的访问次数
    val rdd07: RDD[(Long, Long)] = rdd01.filter(t => {
      tjList.init.contains(t.page_id.toLong)
    }).map(tp => {
      (tp.page_id.toLong, 1L)
    }).reduceByKey(_ + _)
    val map = rdd07.collect().toMap
//    rdd06.collect().foreach(println)
    rdd06.foreach {
      case ((t1, t2), t3) => {
        val maybeLong =map.get(t1).get
//        println(maybeLong)
        println(s"页面${t1}到页面${t2}的页面转化率为：" + (t3.toDouble / maybeLong))
      }
    }
  }

}


