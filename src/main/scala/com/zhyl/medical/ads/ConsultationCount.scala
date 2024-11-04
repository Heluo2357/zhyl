package com.zhyl.medical.ads

import operateHive._
import operateTable._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

class ConsultationCount(date: String) {
   val inquiry: DataFrame = readTable(DWS,"consult_patient_info").cache()


  // TODO: 每个月患者数量
  def monthConsultationSum(): Unit = {
    val inquiryDF = inquiry
      .withColumn("each_month", date_format($"consultation_time", "yyyy-MM"))
    val df = groupCount(inquiryDF, "each_month", "consultation_count")

//    writerAds(df,"month_consultation")
    df.show()
  }

  //TODO: 患者男女比例统计
  def gradesConsultationSum(): Unit = {
    val genderRatioDF = groupCount(inquiry, "patient_gender", "gender_count")
      .select($"patient_gender" as "gender",$"gender_count")

//    writerAds(genderRatioDF,"grades_consultation")
//    writeMysql(genderRatioDF,"grades_consultation")
  genderRatioDF.show()
  }

  //TODO: 患者年龄阶段统计
  def ageGradesConsultationSum(): Unit = {
    val ageGradesDF = groupCount(inquiry, "age_group", "age_gender_count")

//    writerAds(ageGradesDF ,"age_grades_consultation")
//    writeMysql(ageGradesDF ,"age_grades_consultation")
    ageGradesDF.show()
  }

}

