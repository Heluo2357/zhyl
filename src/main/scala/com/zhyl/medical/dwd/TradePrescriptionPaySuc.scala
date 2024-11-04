package com.zhyl.medical.dwd

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DecimalType, LongType}

// TODO:交易域处方开单支付成功事务事实表
class TradePrescriptionPaySuc {
  def first_start(date: String): Unit = {
    val  prescription_detail = firstReadOdsTable("prescription_detail_inc",date)
    val prescription =  firstReadOdsTable("prescription_inc",date)
      .where($"data.status"==="203")
     dispose(prescription_detail,prescription)

  }

  def days_start(date: String): Unit = {
    val prescription_detail = daysReadOdsTable("prescription_detail_inc",date)
    val prescription =  spark.table("ods.prescription_inc")
      .where($"dt"=== date && $"type"=== "update" && $"data.status"==="203")

    dispose(prescription_detail, prescription)
  }


  def dispose(leftDF:DataFrame,rightDF:DataFrame): Unit ={
    val prescription_detail = leftDF.select($"data.id",
        $"data.count",
        $"data.medicine_id",
        $"data.prescription_id")

    val prescription = rightDF.filter($"data.status" === "203")
      .select($"data.id",
        $"data.total_amount",
        $"data.update_time" as "prescription_pay_suc_time",
        $"data.consultation_id",
        $"data.doctor_id",
        $"data.patient_id")


    val df = prescription_detail.as("leftDF")
      .join(prescription as "rightDF", $"leftDF.prescription_id" === $"rightDF.id")
      .select(
        $"leftDF.id",
        $"prescription_pay_suc_time",
        $"count" cast LongType as "count",
        $"medicine_id",
        $"prescription_id",
        $"total_amount" cast DecimalType(16, 2) as "total_amount",
        $"consultation_id",
        $"doctor_id",
        $"patient_id")

    writerDwd(df, "trade_prescription_pay_suc_inc", "prescription_pay_suc_time")

    df.show()

  }

}
