package com.wbx.spark.benchmark

import com.wbx.entity.CallHistory
import com.wbx.utils.CallHistoryFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util

object SparkWriteHDFSOrc {

  def main(args: Array[String]): Unit = {
    //spark设置
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("SparkWriteHDFSOrc").setMaster("local[1]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //创建100w条数据
    val list : util.ArrayList[Row] = new util.ArrayList[Row]()
    for(_ <- 1 to 1000000){
      list.add(converter(CallHistoryFactory.build()))
    }
    //创建test.call_history_orc表
    val df : DataFrame = spark.createDataFrame(list, callHistorySchema())
    val sql = "CREATE TABLE IF NOT EXISTS test.call_history_orc (" +
      "impi_from STRING," +
      "impi_to STRING," +
      "call_time BIGINT," +
      "call_duration INT," +
      "impi_from_location STRING)" +
      "USING orc"
    spark.sql(sql)
    //写入
    val startTime = System.currentTimeMillis()
    df.write.mode(SaveMode.Append).format("orc").save("hdfs://192.168.137.104:9000/user/hive/warehouse/test.db/call_history_orc")
    val endTime = System.currentTimeMillis()
    System.out.println("执行时间为：" + (endTime - startTime) + "ms")
  }

  private def converter(callHistory: CallHistory): Row = {
    val impiFrom = callHistory.getImpiFrom()
    val impiTo = callHistory.getImpiTo
    val callTime = callHistory.getCallTime
    val callDuration = callHistory.getCallDuration
    val impiFromLocation = callHistory.getImpiFromLocation
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

