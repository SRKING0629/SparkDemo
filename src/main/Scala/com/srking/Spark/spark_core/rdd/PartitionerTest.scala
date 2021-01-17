package com.srking.Spark.spark_core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object PartitionerTest {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("CoalesceTest")
      val context = new SparkContext(conf)
      val rdd01 = context.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(6,"e"),(8,"f")))
      val result = rdd01.partitionBy(new SelfPartitioner(2))
      result.saveAsTextFile("out")
        context.stop()
    }
}
