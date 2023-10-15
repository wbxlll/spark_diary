package com.wbx.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/4/5
 */
public class JavaReadTest {

    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("Car Num Analyse")
                .setMaster("local[2]")
                .set("hive.metastore.uris", "thrift://192.168.137.104:9083");
        SparkSession session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        Dataset<Row> result = session.sql("select * from stu");
        result.show();
    }

}
