package com.zhyl.medical.dws

import operateHive._
import operateTable.calculateAgeAndGroup
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

class ConsultPatientInfo(date: String) {
  val doctorDF: DataFrame = daysReadDimTable("doctor_full", date)
    .select($"id", $"hospital_id").dropDuplicates

  val hospitalDF: DataFrame = daysReadDimTable("hospital_full", date)
    .select($"id", $"street").dropDuplicates

  val patientDF: DataFrame = daysReadDimTable("patient_full", date)
    .select($"id", $"gender", $"birthday").dropDuplicates

  def first_start(): Unit = {
    val consultDF = firstReadDwdTable("trade_consultation_inc", date)
    dispose(consultDF)
  }

  def days_start(): Unit = {
    val consultDF = daysReadDwdTable("trade_consultation_inc", date)
    dispose(consultDF)
  }


  def dispose(consultDF: DataFrame): Unit = {

    val combineDF = doctorDF.join(hospitalDF, doctorDF("hospital_id") === hospitalDF("id"))
      .select(doctorDF("id") as "doctor_id", $"street")


    val consultPatientDF = consultDF.join(combineDF, consultDF("doctor_id") === combineDF("doctor_id"), "left")
      .select($"id", $"consultation_time", $"patient_id", $"street")
      .join(patientDF, consultDF("patient_id") === patientDF("id"), "left")
//
    val consultInfoDF = calculateAgeAndGroup(consultPatientDF, date)
      .withColumn("dt", date_format($"consultation_time", "yyyy-MM-dd"))
      .select(consultDF("id"), $"consultation_time", $"patient_id", $"gender" as "patient_gender", $"age" as "patient_age", $"street", $"age_group", $"dt")

    //
//    val df = calculateAgeAndGroup(consultInfoDF, date)
//      .select($"id", $"consultation_time", $"patient_id", $"gender" as "patient_gender", $"age" as "patient_age", $"street", $"age_group", $"dt")

    writerDws(consultInfoDF, "consult_patient_info")
    consultInfoDF.show()
  }
}
