package com.srking.Spark.utils

import java.util

import org.apache.hadoop.hbase.util.Bytes
import redis.clients.jedis.{JedisCluster, _}

import scala.xml.XML

/**
  * 在使用jedisCluster的时候需要注意的事项
  *   用jedis的JedisCluster.close()方法造成的集群连接关闭的情况
  *   jedisCluster内部使用了池化技术，每次使用完毕都会自动释放Jedis因此不需要关闭
  *
  *   只能在clust机器上面去设置哟^-^
  */
object RedisClusterMethodUtil{


  val confFile = XML.loadFile("mgh.xml")

  // redis集群的ip地址和端口 逗号隔开
  val redis_ip = (confFile \\ "redis_ip").text

  // 连接redis的密码
  val password = (confFile \\ "password").text

  // 连接超时时间  -->  不能连接太久
  val timeout = (confFile \\ "timeout").text.toInt

  // 最大尝试连接次数
  val max_attempts = (confFile \\ "max_attempts").text.toInt

  // 表示读取数据超时时间 --> 就是不能读一个数据读太久
  val so_timeout = (confFile \\ "so_timeout").text.toInt

  // 初始化jedisCluster的对象
  var jedisCluster:JedisCluster = _

  // 配置jedis
  val poolConf: JedisPoolConfig = new JedisPoolConfig
  //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
  poolConf.setBlockWhenExhausted(true)

  //设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
  poolConf.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy")

  //是否启用pool的jmx管理功能, 默认true
  poolConf.setJmxEnabled(true)

  //是否启用后进先出, 默认true
  poolConf.setLifo(true)

  //最大连接数, 默认8个
  poolConf.setMaxTotal(16)

  //最大空闲连接数, 默认8个
  poolConf.setMaxIdle(16)

  //获取连接时的最大等待毫秒数
  poolConf.setMaxWaitMillis(6000)

  //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
  poolConf.setMinEvictableIdleTimeMillis(1800000)

  //最小空闲连接数, 默认0
  poolConf.setMinIdle(0)

  //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
  poolConf.setNumTestsPerEvictionRun(3)

  //在获取连接的时候检查有效性, 默认false//在获取连接的时候检查有效性, 默认false

  poolConf.setTestOnBorrow(false)

  //在空闲时检查有效性, 默认false
  poolConf.setTestWhileIdle(false)

  val nodes = new util.LinkedHashSet[HostAndPort]
  val hostAndPorts = redis_ip.split(",")
  for (hostAndPort <- hostAndPorts) {
    val hp = hostAndPort.split(":")
    nodes.add(new HostAndPort(hp(0), hp(1).toInt))
  }
  jedisCluster = new JedisCluster(nodes, timeout, so_timeout, max_attempts, password, poolConf)

  /**
    *  设置hash结构 参数为map
    * @param key
    * @param fieldValues
    */
  def setCols(
               key: String,
               fieldValues: Map[String, String]
             ): Unit = {
    import scala.collection.JavaConverters._
    val data = fieldValues.map(element => {
      (element._1.getBytes(), element._2.getBytes())
    }).asJava
    jedisCluster.hmset(key.getBytes(), data)
  }

  /**
    * 获取hash结构的数据
    * @param key
    * @param cols
    * @return Map
    */
  def getCols(key: String,
              cols: Array[String] = Array.empty
             ): Map[String, Array[Byte]] = {
    import scala.collection.JavaConverters._
    var map = Map.empty[String, Array[Byte]]
    if (cols.length > 0) {
      val response: util.List[Array[Byte]] = jedisCluster.hmget(key.getBytes(), cols.map(_.getBytes()): _*)
      map = cols.zip(response.asScala).toMap.filter(x => x._2 != null)

    } else {
      println(s"key: ${key}")
      val tmpMap: util.Map[Array[Byte], Array[Byte]] = jedisCluster.hgetAll(key.getBytes())
      map = tmpMap.asScala.toMap.map(x => (new String(x._1),x._2))
    }
    map
  }

  /**
    * 删除hash结构的数据
    * @param key
    * @param list
    */
  def delteCols(
                 key: String,
                 list: List[String]
               ): Unit = {

    for (i <- 0 to list.size - 1){
      jedisCluster.hdel(key,list(i))
    }
  }

  /**
    * 获取redis hkey中所有key
    * @param key
    * @return
    */
  def getKeys(    key: String
             ): List[String] = {

    import scala.collection.JavaConversions._

    val keysList: util.Set[String] = jedisCluster.hkeys(key)

    keysList.toList
  }

  /**
    *  判断hash结构的数据是否存在
    * @param key
    * @return
    */
  def existsCols(
                  key: String
                ): Boolean = {

    val flag = jedisCluster.exists(key)

    flag
  }

  /**
    * 自增hash结构里面的数据
    * @param key
    * @param field
    * @param value
    */
  def incresCols(key: String, field: String, value: Double): Unit = {

    jedisCluster.hincrByFloat(key.getBytes(),field.getBytes(),value)

  }

  /**
    * 获取k-v类型的数据
    * @param key
    * @return
    */
  def get(key: String): String = {

    val value: String = jedisCluster.get(key)
    value
  }

  /**
    * 设置k-v结构的数据
    * @param key
    * @param value
    */
  def set(key: String,value: String): Unit = {

    jedisCluster.set(key, value)

  }

  def main(args: Array[String]): Unit = {

    //    getRedisClusterw

    // 设置一个key
//    set("tmp_wfw_kv","18725908515")
//    println("get value ->" + get("tmp_wfw_kv"))
//
//    val map = Map("name"-> "weifuwan","age"-> "26", "gender"-> "male", "id"-> "519099386", "mel"->"333")
//    // 设置一个hash数据结构
//    setCols("tmp_wfw_hash",map)
//
//    val Hvalue = getCols("tmp_wfw_hash")
//
//    Hvalue.foreach(p => println("hashValue" + p._1,new String(p._2)))
//
//    println("key->" + existsCols("tmp_wfw_hash"))
//
//    incresCols("tmp_wfw_hash","bruce",2)

  }

}