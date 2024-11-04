package com.zhyl.medical.dws

import operateHive.{daysReadDimTable, daysReadDwdTable, firstReadDwdTable}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

class DrugYearMonth(date: String) {


  def first_start(): Unit = {
    val consultDF = firstReadDwdTable("trade_prescription_pay_suc_inc", date)
    dispose(consultDF)
  }

  def days_start(): Unit = {
    val consultDF = daysReadDwdTable("trade_prescription_pay_suc_inc", date)
    dispose(consultDF)
  }

  def dispose(df: DataFrame): Unit = {
    val medicineDF: DataFrame = daysReadDimTable("medicine_full", date)

    val drugDf = df.as("df")
      .join(medicineDF as "m", $"df.medicine_id" === $"m.id", "left")
      .select($"name" as "drug", $"df.dt")
      .withColumn("year", year($"dt"))
      .withColumn("month", month($"dt"))
      .groupBy($"drug", $"year", $"month", $"dt")
      .agg(count("*") as "drug_count")

    drugDf.show()

    writerDws(drugDf, "drug_year_month")
  }


}
