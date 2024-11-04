package com.zhyl.medical

import com.zhyl.medical.function.{OperateHive, OperateTable}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

package object ads {
  System.setProperty("HADOOP_USER_NAME", "heluo")

  val DWS = "dws"

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("medical")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  def writerAds(df: DataFrame, table: String): Unit = {
    df.write.format("hive")
      .mode(SaveMode.Overwrite)
      .option("delimiter", "\t")
      .csv(s"/user/hive/warehouse/ads.db/${table}")

    //    df.write.format("jdbc")
    //      .mode(SaveMode.Overwrite)
    //      .option(JDBCOptions.JDBC_URL, "jdbc:mysql://192.168.45.47:3306/medical?useSSL=false&user=root&password=jiang2357&characterEncoding=utf-8&allowPublicKeyRetrieval=true")
    //      .option(JDBCOptions.JDBC_DRIVER_CLASS, "com.mysql.cj.jdbc.Driver")
    //      .option(JDBCOptions.JDBC_TABLE_NAME, table)
    //      .save()

    println(s"加载ads:${table}数据成功")
  }

  val operateHive = new OperateHive(spark)
  val operateTable = new OperateTable(spark)

}
