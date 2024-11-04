package com.zhyl.medical.dws

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class HospitalConsultation1d(date: String) {

  val hospitalDF: DataFrame = daysReadDimTable("hospital_full", date)
    .select($"id", $"name", $"street")

  val doctorDF: DataFrame = daysReadDimTable("doctor_full", date)
    .select($"id", $"hospital_id")

  def first_start(): Unit = {
    val consultDF = firstReadDwdTable("trade_consultation_inc", date)
    dispose(consultDF)
  }

  def days_start(): Unit = {
    val consultDF = daysReadDwdTable("trade_consultation_inc", date)
    dispose(consultDF)
  }

  def dispose(consultDF: DataFrame): Unit = {

    val combineDF = doctorDF.join(hospitalDF, doctorDF("hospital_id") === hospitalDF("id"), "right")
      .select(doctorDF("id") as "doctor_id", $"hospital_id", $"name" as "hospital_name", $"street")

    val df = consultDF.join(combineDF, Seq("doctor_id"), "left")
      .groupBy($"hospital_id", $"hospital_name", $"street", $"dt")
      .agg(count("hospital_id") as "consultation_count")
      .withColumn("consultation_count", nanvl($"consultation_count", lit(0)))


    df.show()

//    writerDws(df, "hospital_consultation_1d")
  }


}
