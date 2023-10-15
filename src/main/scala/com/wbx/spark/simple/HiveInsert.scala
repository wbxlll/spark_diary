package com.wbx.spark.simple

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD

object HiveInsert {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Hive Insert")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.137.104:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();

    val seq: Seq[(String, String, String)] = Seq(("Bob", "Alice" ,"BEIJING"), ("Alice", "Bob", "HENAN"))
    val rdd: RDD[(String, String, String)] = spark.sparkContext.parallelize(seq)
    val df: DataFrame = spark.createDataFrame(rdd)
    df.write.mode(SaveMode.Append).insertInto("test.call")
    val df2: DataFrame = spark.read.table("test.call")
    df2.show()
  }

}
