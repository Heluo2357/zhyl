package com.zhyl.medical.dws

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class HospitalConsultationNd(date: String) {

  def first_start(): Unit = {
    val consult1d = firstReadDwsTable("hospital_consultation_1d", date)
    dispose(consult1d)
  }

  def days_start(): Unit = {

    val consult1d = daysReadDwsTable("hospital_consultation_1d", date)

    val consultNd = readTable(DWS, "hospital_consultation_nd")
      .filter($"dt" === date_sub(lit(date), 1))

    val combinedDF = consultNd.union(consult1d)

    dispose(combinedDF)
  }

  def dispose(consultDF: DataFrame): Unit = {

    val df = consultDF.groupBy($"hospital_id", $"hospital_name", $"street")
      .agg(sum($"consultation_count") as "consultation_count")
      .withColumn("dt", lit(date))

    df.show()


//    writerDws(df, "hospital_consultation_nd")
  }
}
