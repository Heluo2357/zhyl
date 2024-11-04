package com.zhyl.medical.ads

import operateHive._
import operateTable._
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class Synthesize(date: String) {
  val inquiry: DataFrame = readTable(DWS, "consult_patient_info").cache()
  val hospital: DataFrame = daysReadDimTable("hospital_full", date)

  // TODO:总患者人数，医护人员人数，医院数量，科室数量， 床位数量，日均就诊人数
  def start(): Unit = {
    // 全国患者人数
    val patientNumDF = countFunc(inquiry, "patient_num")
    // 医护人员人数
    val doctorNumDF = countFunc(daysReadDimTable("doctor_full", date), "doctor_num")

    // 医院数量
    val hospitalNumDF = countFunc(hospital, "hospital_num")

    //科室数量
    val departmentNumDF = hospital.agg(sum($"department_num").as("department_num"))

    // 床位数量
    val bedNumDF = hospital.agg(sum($"bed_num").as("bed_num"))

    //统计每日就诊人数
    val daily = inquiry.groupBy($"dt")
      .agg(countDistinct($"id").as("daily_consultation_count"))

    //日均就诊人数
    val avgDailyConsultationCountDF = daily.agg(round(avg($"daily_consultation_count"), 0) cast LongType as "avg_daily_consultation_count")

    val combinedDF = patientNumDF
      .crossJoin(doctorNumDF)
      .crossJoin(hospitalNumDF)
      .crossJoin(departmentNumDF)
      .crossJoin(bedNumDF)
      .crossJoin(avgDailyConsultationCountDF)


    writerAds(combinedDF, "synthesize")
    combinedDF.show()
    //    writeMysql(combinedDF,"synthesize")
  }
}
