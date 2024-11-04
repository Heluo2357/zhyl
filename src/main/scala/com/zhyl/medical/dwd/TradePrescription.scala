package com.zhyl.medical.dwd

import operateHive._
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DecimalType, LongType}
// TODO 交易域处方开单事务事实表
class TradePrescription {

  // TODO:首日数据加载方法
  def first_start(date: String): Unit = {

    val prescriptionDF = firstReadOdsTable("prescription_inc", date)
    val prescriptionDetDF = firstReadOdsTable("prescription_detail_inc", date)

    dispose(prescriptionDF,prescriptionDetDF)

}

  // TODO:每日数据加载方法

  def days_start(date: String): Unit = {

    val prescriptionDF = daysReadOdsTable("prescription_inc", date)
    val prescriptionDetDF = daysReadOdsTable("prescription_detail_inc", date)

    dispose(prescriptionDF, prescriptionDetDF)
  }

  def dispose(leftDF:DataFrame,rightDF:DataFrame): Unit ={
    val prescriptionDF = leftDF
      .select($"data.id",
        $"data.create_time",
        $"data.consultation_id",
        $"data.doctor_id",
        $"data.patient_id",
        $"data.total_amount")

    val prescriptionDetDF = rightDF
      .select($"data.count",
        $"data.medicine_id",
        $"data.prescription_id")

    val df = prescriptionDF.as("left")
      .join(prescriptionDetDF as "right", $"left.id" === $"right.prescription_id", "left")
      .select($"id",
               $"create_time"  as "prescription_time",
                $"count" cast LongType as "count",
                $"medicine_id",
                $"prescription_id",
                $"total_amount" cast DecimalType(16, 2) as "total_amount",
                $"consultation_id",
                $"doctor_id",
                $"patient_id"
              )
////
    writerDwd(df, "trade_prescription_inc", "prescription_time")
    df.show
  }

}
