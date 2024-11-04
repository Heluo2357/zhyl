package com.zhyl.medical.dwd

import spark.implicits._
import operateHive._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

// TODO: 交易域问诊支付成功事务事实表

class TradeConsultationPaySuc {


  def first_start(date: String): Unit = {
    // 所有诊断信息
    val consulDF = firstReadOdsTable("consultation_inc", date)
      .select($"data.id",
        $"data.consultation_fee",
        $"data.doctor_id",
        $"data.patient_id",
        $"data.user_id")

    //所有支付成功记录
    val paymentDF = firstReadOdsTable("payment_inc",date)
      .where($"data.status" ==="203") // status为203表示支付成功
      .select($"data.consultation_id",$"data.update_time") //update_time支付成功时间

    val df = consulDF.as("consul")
      .join(paymentDF as "payment",$"consul.id" === $"payment.consultation_id")
      .select($"id",
        $"update_time" as "consultation_pay_suc_time",
        $"consultation_fee" cast  DecimalType(16, 2) as "consultation_fee",
        $"doctor_id",
        $"patient_id",
        $"user_id")

    writerDwd(df,"trade_consultation_pay_suc_inc","consultation_pay_suc_time")
    df.show()
  }

  // TODO:每日数据加载方法
  def days_start(date: String): Unit = {
    val consulDF = spark.table("ods.consultation_inc")
      .select($"data.id",
        $"data.update_time" as "consultation_pay_suc_time",
        $"data.consultation_fee" cast DecimalType(16, 2) as "consultation_fee",
        $"data.doctor_id",
        $"data.patient_id",
        $"data.user_id")
      .filter($"dt" === date && $"type"=== "update" &&
        array_contains(map_keys(col("old")), "status") && col("data.status") === "203")
    //诊断支付成功对ods_consultation_inc的影响，更新status=203 代表支持成功了



    writerDwd(consulDF, "trade_consultation_pay_suc_inc", "consultation_pay_suc_time")

    consulDF.show()
  }

}
