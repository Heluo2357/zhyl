package com.zhyl.medical.dwd


import operateHive._
import org.apache.spark.sql.DataFrame
import spark.implicits._

class InteractionReview {

  def first_start(date:String): Unit ={
    val reviewDF = firstReadOdsTable("consultation_inc",date)
    dispose(reviewDF)

  }

  def days_start(date:String): Unit ={
    val review = readOdsTable("consultation_inc",date)
      .where( $"type" === "update")
    dispose(review)
  }


  def dispose(patientDF: DataFrame): Unit = {
    val df = patientDF
      .where($"data.status" === "207")
      .select($"data.id",
      $"data.update_time" as "review_time",
      $"data.rating",
      $"data.doctor_id",
      $"data.patient_id",
      $"data.user_id")
    writerDwd(df, "interaction_review_inc", "review_time")
    df.show
  }

}

