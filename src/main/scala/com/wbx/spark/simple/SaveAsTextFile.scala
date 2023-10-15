package com.wbx.spark.simple

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SaveAsTextFile {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession
      .builder()
      .appName("SaveAsTextFile")
      .master("local[2]")
      .getOrCreate();

    val modelNames = Array("FM", "FFM", "DEEPFM", "NFM", "DIN", "DIEN")
    //分几片就会产生几个文件
    val modelNamesRdd = spark.sparkContext.parallelize(modelNames, 1)
    modelNamesRdd.saveAsTextFile("/user/root/test4", classOf[org.apache.hadoop.io.compress.SnappyCodec])
//    spark.read.format()
//    val file:RDD[String] = spark.sparkContext.textFile("/user/root/test2")
//    file.foreach(println)
  }

}
