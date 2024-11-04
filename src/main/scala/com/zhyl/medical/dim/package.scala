package com.zhyl.medical

import com.zhyl.medical.function.OperateHive
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


package object dim {
  System.setProperty("HADOOP_USER_NAME", "heluo")

  val ODS = "ods"
  val DIM = "dim"

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("medical")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._


  def writeOdsToDim(table: String, partition: String): Unit = {
    val df = spark.table(s"ods.${table}")
      .filter($"dt" === partition)
      .drop("create_time", "update_time")

    writerDim(df, table, partition)
  }

  def writerDim(df: DataFrame, table: String, partition: String): Unit = {
    df.withColumn("dt", lit(partition))
      .write.format("hive")
      .mode(SaveMode.Append)
      .partitionBy("dt")
      .saveAsTable(s"dim.$table")

    println(s"dim.${table}加载数据成功")
  }

  val operateHive = new OperateHive(spark)
}
