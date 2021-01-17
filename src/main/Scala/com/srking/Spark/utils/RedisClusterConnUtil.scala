package com.srking.Spark.utils

import java.util

import redis.clients.jedis.{JedisCluster, _}

import scala.xml.XML

/**
  * 在使用jedisCluster的时候需要注意的事项
  *   用jedis的JedisCluster.close()方法造成的集群连接关闭的情况
  *   jedisCluster内部使用了池化技术，每次使用完毕都会自动释放Jedis因此不需要关闭
  *
  *   只能在clust机器上面去设置哟^-^
  */
object RedisClusterConnUtil {


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
  var jedisCluster: JedisCluster = _

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


  def getRedisConn(): JedisCluster = {
    jedisCluster
  }
}

