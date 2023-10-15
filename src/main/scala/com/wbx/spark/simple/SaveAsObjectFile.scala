package com.wbx.spark.simple

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SaveAsObjectFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SaveAsTextFile")
      .master("local[2]")
      .getOrCreate();

    val modelNames = Array("FM", "FFM", "DEEPFM", "NFM", "DIN", "DIEN")
    //序列化这个rdd对象到文件中
    val modelNamesRdd : RDD[String] = spark.sparkContext.parallelize(modelNames, 1)
    modelNamesRdd.saveAsObjectFile("/user/root/test3")
  }
}
