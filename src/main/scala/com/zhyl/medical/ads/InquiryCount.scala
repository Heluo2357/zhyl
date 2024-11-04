package com.zhyl.medical.ads

import operateHive._
import operateTable._
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import scala.language.postfixOps

class InquiryCount(date: String) {

  val inquiry: DataFrame = readTable(DWS, "consult_patient_info").cache()

  //TODO :平均月就诊次数 总就诊次数
  def consultationSum(): Unit = {
    val inquiryDF = inquiry
      .withColumn("year_month", date_format($"consultation_time", "yyyy-MM"))

    val monthlyConsultationCountDF = groupCount(inquiryDF, "year_month", "monthly_consultation_count")

    //月均就诊次数
    val avgMonthlyConsultationCountDF = monthlyConsultationCountDF
      .agg(round(avg("monthly_consultation_count") cast LongType as ("avg_monthly_consultation_count"), 0) as "monthly_count")

    // 就诊次数
    val consultationCount = inquiry.agg(count("*") as "total_count")

    val combinedDF = avgMonthlyConsultationCountDF.crossJoin(consultationCount)

    writerAds(combinedDF, "monthly_and_total_inquiry")

    combinedDF.show()
    //    writeMysql(combinedDF,"monthly_and_total_inquiry")

  }

  //  //TODO 药品类使用次数统计
  def drugClassInfoSum(): Unit = {

    val drugDF = daysReadDwsTable("drug_class_info_nd", date).where($"drug" isNotNull)
      .drop($"dt")
    drugDF.show()

    writerAds(drugDF, "drug_class")
    //      writeMysql(drugDF ,"drug_class")
  }

  //TODO: 科室挂号次数，问诊成功次数
  def departmentInquirySum(): Unit = {
    //科室挂号次数
    val department = readTable(DWS, "department_registration")
    //科室问诊成功次数
    val departmentSuc = readTable(DWS, "department_registration_suc")

    val departmentDF = department.join(departmentSuc, "department")
      .groupBy($"department")
      .agg(
        sum($"consultation_count") as "registration_count",
        sum($"consultation_suc_count") as "department_suc_count"
      )

    departmentDF.show()

    writerAds(departmentDF, "department_inquiry")
    //    writeMysql(departmentDF,"department_inquiry")
  }

  def drug_season(): Unit = {
    val drugDF = readTable(DWS, "drug_year_month")

    /*val df = drugDF.withColumn("season", when($"month".isin(12, 1, 2), "冬季")
      .when($"month".isin(3, 4, 5), "春季")
      .when($"month".isin(6, 7, 8), "夏季季")
      .when($"month".isin(9, 10, 11), "秋季")
      .otherwise("未知季节"))
      .groupBy($"month", $"drug").agg(sum($"drug_count") cast LongType as "drug_count")
      .orderBy($"drug_count" desc)
      .limit(10)*/

    val df = drugDF.withColumn("season", monthToSeasonUDF($"month"))
      .groupBy($"season", $"drug").agg(sum($"drug_count") cast LongType as "drug_count")
      .orderBy($"drug_count" desc)
      .limit(10)


    df.show()
    writerAds(df, "drug_season")
  }
}


