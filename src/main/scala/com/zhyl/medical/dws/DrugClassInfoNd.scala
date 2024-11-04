package com.zhyl.medical.dws

import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{date_add, lit, sum}
import spark.implicits._

class DrugClassInfoNd(date:String) {

  def first_start(): Unit = {
    val tradeDF = firstReadDwsTable("drug_class_info_1d", date)
    dispose(tradeDF)
  }

  def days_start(): Unit ={
    val drug1d = daysReadDwsTable("drug_class_info_1d", date)

    val drugNd = readTable(DWS, "drug_class_info_nd")
      .filter($"dt" === date_add(lit(date), -1))

    val drugDF =  drug1d.union(drugNd)
    dispose(drugDF)

  }

  def dispose(drugDF: DataFrame): Unit = {

   val df =  drugDF.groupBy($"drug")
      .agg(sum($"drug_class_count") as "drug_class_count")
     .withColumn("dt",lit(date))

    writerDws(df,"drug_class_info_nd")
    df.show()
  }
}
