package com.wbx.spark.utils

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.DataFrame

import java.lang

/**
 * @Author xsh
 * @Date : 2020-3-20 17:06
 * @Description: 连接/配置参数工具类
 */


object ConnectUtils {

  /**
   * mysql连接配置
   */
  lazy val mysqlConnect: (String, String, String, String, String, String) =>
    DataFrame = (ip: String, port: String, database: String, table: String, userName: String, password: String) => {
    SparkUtils.sparkSessionWithHive.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://$ip:$port?useUnicode=true&characterEncoding=utf-8&useSSL=false")
      .option("dbtable", s"$database.$table")
      .option("user", userName)
      .option("password", password)
      .load()
  }

  /**
   * kafka消费者配置
   */
  val kafkaConsumerConfig: (String, String) => Map[String, Object] = (bootstrapServers: String, group: String) => {
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
  }

  /**
   * kafka生产者配置
   */
  val kafkaProducerConfig: String => Map[String, Object] = (bootstrapServers: String) => {
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer]
    )
  }

}


