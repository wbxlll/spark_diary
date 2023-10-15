package com.wbx.spark.streaming

import com.alibaba.fastjson2.JSONObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

object Kafka2HiveOrc {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("Kafka2HiveOrc")
      .setMaster("local[2]")
      .set("spark.sql.warehouse.dir", "hdfs://192.168.137.104:9000/user/hive/warehouse")
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") //todo 10000
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val topic = Array("test")
    //kafka 参数
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "host4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "Kafka2HiveOrc",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc = ss.sparkContext
    val ssc = new StreamingContext(sc, Seconds(30)) //这个是重点微批处理，根据自己的机器资源，测试调整

    val kfStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap)
    )

    val sql = "CREATE TABLE IF NOT EXISTS test.call_history_orc (" +
      "impi_from STRING," +
      "impi_to STRING," +
      "call_time BIGINT," +
      "call_duration INT," +
      "impi_from_location STRING)" +
      "USING orc"
    ss.sql(sql);
    //手动提交offset
    kfStream.foreachRDD(
      rdd => {
        //记录offset
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        if (!rdd.isEmpty()) {
          val rowRDD: RDD[Row] = rdd.map(x => jsonConvert(JSONObject.parseObject(x.value())))
          val dataDf: DataFrame = ss.createDataFrame(rowRDD, callHistorySchema()).repartition(1)
          dataDf.show()
          dataDf.write.mode(SaveMode.Append).format("orc").saveAsTable("test.call_history_orc")
        }
        //手动提交offset
        kfStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
    ssc.start()
    ssc.awaitTermination()
  }

  private def jsonConvert(json: JSONObject): Row = {
    val impiFrom = json.getString("impiFrom")
    val impiTo = json.getString("impiTo")
    val callTime = json.getLongValue("callTime")
    val callDuration = json.getIntValue("callDuration")
    val impiFromLocation = json.getString("impiFromLocation")
    Row(impiFrom, impiTo, callTime, callDuration, impiFromLocation)
  }

  def callHistorySchema(): StructType = {
    StructType(Array(StructField("impi_from", StringType),
      StructField("impi_to", StringType),
      StructField("call_time", LongType),
      StructField("call_duration", IntegerType),
      StructField("impi_from_location", StringType)))
  }
}

