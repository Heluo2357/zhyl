package com.zhyl.medical

import com.zhyl.medical.function.{OperateHive, OperateTable}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.date_format

package object dwd {

  System.setProperty("HADOOP_USER_NAME", "heluo")

  val DWD = "dwd"
  val ODS = "ods"

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("medical")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._


  def writerDwd(df: DataFrame, table: String, partitionField: String): Unit = {
    df.withColumn("dt", date_format($"${partitionField}", "yyyy-MM-dd"))
      .write.format("hive")
      .mode(SaveMode.Append)
      .partitionBy("dt")
      .saveAsTable(s"dwd.${table}")

    println(s"加载ods-dwd:${table}数据成功")
  }

  val operateHive = new OperateHive(spark)
  val operateTable  = new OperateTable(spark)

}
