package com.srking.Spark.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.log4j.Logger

import scala.xml.XML

object HBaseUtil {
  private val logRoot:Logger = Logger.getRootLogger
  //读取配置文件
  val conf = XML.loadFile("mgh.xml")
  //HBase连接信息
  val zookeeper = (conf \\ "zookeeper").text.toString

  //创建一个连接对象,配置hbase节点参数
  val configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum", zookeeper)
  configuration.set("hbase.regionserver.handler.count","100")
  configuration.set("hfile.block.cache.size","0.1")
  configuration.set("hbase.regionserver.global.memstore.upperLimit","0.5")
  configuration.set("hbase.regionserver.global.memstore.lowerLimit","0.45")
  configuration.set("hbase.zookeeper.property.maxClientCnxns","400")
  // 偶尔需要这个
  configuration.set("zookeeper.znode.parent","/hbase-unsecure")

  // 是操作hbase的入口
  val connection: Connection = ConnectionFactory.createConnection(configuration)

  // 获取一个连接
  def getConnection():Connection = {
    connection
  }

}