package com.srking.Spark.spark_core.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


object Test02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test01").setMaster("local[*]")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val frame = ss.read.json("FileIn/test02.json")
    frame.createOrReplaceTempView("test02")
//    ss.udf.register("add", (name: String) => {
//      "name" + name
//    })
//    ss.sql("select  add(name) from test02")
    ss.udf.register("avg", new avgUdaf())
    ss.sql("select avg(age) from test02 ").show
  }
}

class avgUdaf extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    StructType(
      Array(StructField("age", LongType))
    )

  }

  override def bufferSchema: StructType = {
    StructType(
      Array(StructField("totalage", LongType), StructField("cnt", LongType)))
  }

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0L)
    buffer.update(1,0L)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0,buffer.getLong(0)+input.getLong(0))
    buffer.update(1,buffer.getLong(1)+1L)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
    buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)/buffer.getLong(1)
  }
}
