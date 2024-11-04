package com.zhyl.medical

import com.zhyl.medical.function.{OperateHive, OperateTable}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

package object dws {

  System.setProperty("HADOOP_USER_NAME", "heluo")

  val DIM = "dim"
  val DWD = "dwd"
  val DWS = "dws"

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("medical")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  def writerDws(df: DataFrame, table: String): Unit = {
    df.write.format("hive")
      .mode(SaveMode.Append)
      .partitionBy("dt")
      .saveAsTable(s"dws.$table")

    println(s"加载dwd-dws:${table}数据成功")
  }


  def writerDws(df: DataFrame, table: String, date: String): Unit = {
    df.write.format("hive")
      .mode(SaveMode.Append)
      .partitionBy(date)
      .saveAsTable(s"dws.${table}")

    println(s"加载dwd-dws:${table}数据成功")
  }

  val operateHive = new OperateHive(spark)
  val operateTable = new OperateTable(spark)


}
