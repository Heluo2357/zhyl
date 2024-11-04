package com.zhyl.medical.dwd

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DecimalType

// TODO: 交易域问诊事务事实表
class TradeConsultation(){

  def first_start(date:String): Unit ={
    val consulDF = firstReadOdsTable("consultation_inc", date)
    dispose(consulDF)
  }


  def days_start(date:String): Unit ={
    val consulDF = daysReadOdsTable("consultation_inc", date)
  dispose(consulDF)


  }

  def dispose(df: DataFrame): Unit = {
   val consulDF = df.select($"data.id",
      $"data.create_time" as "consultation_time",
      $"data.consultation_fee" cast DecimalType(16, 2) as "consultation_fee",
      $"data.doctor_id",
      $"data.patient_id",
      $"data.user_id")
//
    writerDwd(consulDF, "trade_consultation_inc","consultation_time")
    consulDF.show()
  }

}
