package com.wbx.spark.simple

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SaveAsORCFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SaveAsTextFile")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.137.104:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();

    val modelNames = Array("AFM", "AFFM", "ADEEPFM", "ANFM", "ADIN", "ADIEN")
    //序列化这个rdd对象到文件中
    val modelNamesRDD : RDD[Row] = spark.sparkContext.parallelize(modelNames, 1).map(row => Row.apply(row))
    val df : DataFrame = spark.createDataFrame(modelNamesRDD, callHistorySchema()).repartition(1)
    df.show()
    df.write.mode(SaveMode.Append).format("orc").save("hdfs://192.168.137.104:9000/user/hive/warehouse/test.db/test3")
  }

  def callHistorySchema(): StructType = {
    StructType(Array(StructField("name", StringType)))
  }
}
