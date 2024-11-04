package com.zhyl.medical.dws

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DepartmentRegistrationSuc(date:String) {
  val doctorDF: DataFrame = daysReadDimTable("doctor_full", date)
    .select($"id", $"specialty_name" as "department")

  def first_start(): Unit = {
    val consultDF = firstReadDwdTable("trade_consultation_pay_suc_inc", date)
    dispose(consultDF)
  }

  def days_start(): Unit = {
    val consultDF = daysReadDwdTable("trade_consultation_pay_suc_inc", date)
    dispose(consultDF)
  }

  def dispose(consultDF: DataFrame): Unit = {
    val df = consultDF.join(doctorDF, consultDF("doctor_id") === doctorDF("id"), "left")
      .groupBy($"department", $"dt")
      .agg(
        count("*") as "consultation_suc_count"
      )

    writerDws(df, "department_registration_suc")
    df.show()

  }

}
