package com.zhyl.medical.ads

import operateHive._
import operateTable._
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class HospitalCount(date: String) {


  val consultDF: DataFrame = daysReadDwsTable("hospital_consultation_nd", date).cache()
  val reviewDF: DataFrame = daysReadDwsTable("hospital_review_nd", date).cache()


  //  TODO:  各街道的接诊次数
  def streetSum(): Unit = {
    val df = groupSum(consultDF, "street", "consultation_count", "consultation_count")

    df.show()
    writerAds(df, "street_consultation")
  }

  //TODO: 各医院接诊次数
  def hospitalConsultationSum(): Unit = {
    val frame = consultDF.select($"hospital_name" as "hospital", $"consultation_count")

    frame.show()
  }

  //TODO: 所有医院满意度
  def allHospitalReviewSum(): Unit = {

    val allHospitalReviewDF = reviewDF
      .agg(
        sum("satisfaction") as "satisfaction",
        sum("ordinary") as "ordinary",
        sum("discontent") as "discontent"
      )
    //
    writerAds(allHospitalReviewDF, "all_hospital_review")
    //    writeMysql(allHospitalReviewDF, "all_hospital_review")
    allHospitalReviewDF.show()
  }

  // TODO:各医院好评次数
  def hospitalReviewSum(): Unit = {
    val hospitalCount = reviewDF.select($"name" as "hospital", $"satisfaction" as "good_review_count")
    writerAds(hospitalCount, "hospital_review")
    //      writeMysql(hospitalReviewDF,"hospital_review")
    hospitalCount.show()
  }

}



