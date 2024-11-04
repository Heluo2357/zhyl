package com.zhyl.medical.dws

import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

class HospitalReview1d(date: String)  {
  val doctorDF: DataFrame = daysReadDimTable("doctor_full", date)
    .select($"id", $"hospital_id")

  val hospitalDF: DataFrame = daysReadDimTable("hospital_full", date)
    .select($"id", $"name", $"street")

  def first_start(): Unit = {
    val reviewDF = firstReadDwdTable("interaction_review_inc", date)
      .select($"rating", $"doctor_id", $"dt")

    dispose(reviewDF)
  }

  def days_start(): Unit = {
    val reviewDF = daysReadDwdTable("interaction_review_inc", date)
      .select($"rating", $"doctor_id", $"dt")

    dispose(reviewDF)
  }

  def dispose(reviewDF: DataFrame): Unit = {

    val combineDF = reviewDF
      .join(doctorDF, reviewDF("doctor_id") === doctorDF("id"), "left")
      .select($"hospital_id", $"rating", $"dt")

    val combine = combineDF.groupBy($"dt", $"hospital_id")
      .agg(sum(when($"rating" > 3, 1).otherwise(0)) as "satisfaction",
        sum(when($"rating" === 3, 1).otherwise(0)) as "ordinary",
        sum(when($"rating" < 3, 1).otherwise(0)) as "discontent")

    val df = combine.join(hospitalDF, combine("hospital_id") === hospitalDF("id"))
      .select($"id", $"name", $"street", $"satisfaction", $"ordinary", $"discontent", $"dt")

    writerDws(df, "hospital_review_1d")

    df.show()
  }
}

