package com.wbx.spark.simple

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHiveTable {
  def main(args: Array[String]): Unit = {

      // 创建SparkSession实例
      val spark = SparkSession
        .builder()
        .appName("Read Hive Table")
        .master("local[2]")
        .config("hive.metastore.uris", "thrift://192.168.137.104:9083")
        .config("spark.sql.filesourceTableRelationCacheSize", 0)
        .enableHiveSupport()
        .getOrCreate()
      // 读取Hive表，创建DataFrame


      val df = spark.sql("select count(*) from test.test3")
      df.show()
      Thread.sleep(30000)
      val df2 = spark.sql("select count(*) from test.test3")
      df2.show
      Thread.sleep(30000)
      val df3 = spark.sql("select count(*) from test.test3")
      df3.show

  }

}
