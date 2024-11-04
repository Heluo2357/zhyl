package com.zhyl.medical.function


import org.apache.spark.sql.{DataFrame, SparkSession}

class OperateHive(spark: SparkSession) {

  import spark.implicits._

  def readTable(database: String, table: String): DataFrame = {
    spark.table(s"${database}.${table}")
  }

  def readOdsTable(table: String, partition: String): DataFrame = {
    spark.table(s"${ODS}.${table}")
      .where($"dt" === partition)
  }

  def firstReadOdsTable(table: String, partition: String): DataFrame = {
    spark.table(s"${ODS}.${table}")
      .where($"dt" === partition && $"type" === "bootstrap-insert")
  }

  def daysReadOdsTable(table: String, partition: String): DataFrame = {
   spark.table(s"${ODS}.${table}")
    .where($"dt" === partition && $"type" === "insert")

  }


  def firstReadDwdTable(table: String, partition: String): DataFrame = {
    spark.table(s"${DWD}.${table}")
      .where($"dt" <= partition)
  }

  def daysReadDwdTable(table: String, partition: String): DataFrame = {
    spark.table(s"${DWD}.${table}")
      .where($"dt" === partition)
  }


  def daysReadDimTable(table: String, partition: String): DataFrame = {
    spark.table(s"${DIM}.${table}")
      .where($"dt" === partition)
  }


  def daysReadDwsTable(table: String, partition: String): DataFrame = {
    spark.table(s"${DWS}.${table}")
      .where($"dt" === partition)
  }

  def firstReadDwsTable(table: String, partition: String): DataFrame = {
    spark.table(s"${DWS}.${table}")
      .where($"dt" <= partition)
  }

}
