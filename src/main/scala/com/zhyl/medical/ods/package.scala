package com.zhyl.medical

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

package object ods {
  System.setProperty("HADOOP_USER_NAME", "heluo")

  val ODS = "ods"

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("medical")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  def load_table(table: String, date: String): Unit = {

  spark.sql(s"LOAD DATA INPATH '/origin_data/medical/$table/$date' OVERWRITE INTO TABLE ods.$table PARTITION(dt='$date')")

    spark.sql(s"show partitions ods.$table")
      .orderBy($"partition".desc)
      .limit(3)
      .show()

    spark.sql(s"select * from ods.$table")
      .where($"dt" === date)
      .show(false)

    println(s"ods.${table}加载数据成功")
  }
}
