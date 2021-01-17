package com.srking.Spark.spark_core.rdd

import org.apache.spark.{HashPartitioner, Partitioner}

class SelfPartitioner(nums: Int) extends Partitioner {
  override def numPartitions: Int = {
    nums
  }

  override def getPartition(key: Any): Int = {

    if (key.toString.toInt % 2 == 0)
      0
    else
      1
  }
}
