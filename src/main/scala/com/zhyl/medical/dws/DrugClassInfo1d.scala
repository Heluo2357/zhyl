package com.zhyl.medical.dws

import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, regexp_replace, sum}
import spark.implicits._

class DrugClassInfo1d(date: String) {


  def first_start(): Unit = {
    val tradeDF = firstReadDwdTable("trade_prescription_pay_suc_inc", date)
    dispose(tradeDF)
  }

  def days_start(): Unit = {
    val tradeDF = daysReadDwdTable("trade_prescription_pay_suc_inc", date)
    dispose(tradeDF)

  }

  def dispose(drugDF: DataFrame): Unit = {


    val tradeDF = drugDF.groupBy($"dt", $"medicine_id")
      .agg(count("*").as("medicine_num"))

    val medicineDF = daysReadDimTable("medicine_full", date)
      .withColumn("dose_type", regexp_replace($"dose_type", "\\s*[\\(（﹙][^\\)）﹚]*[\\)）﹚]?", ""))
      .filter($"dose_type" =!= "----" && $"dose_type".isNotNull)
      .select($"id", $"dose_type")


    val resultDF = tradeDF
      .join(medicineDF, tradeDF("medicine_id") === medicineDF("id"), "left")
      .groupBy($"dose_type", $"dt")
      .agg(sum($"medicine_num").as("drug_class_count"))
      .select($"dose_type".as("drug"), $"drug_class_count", $"dt")

    writerDws(resultDF, "drug_class_info_1d")
    resultDF.show()

  }


}
