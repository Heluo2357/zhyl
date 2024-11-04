package com.zhyl.medical.dws

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class HospitalReviewNd(date: String) {

  def first_start(): Unit = {
    val review1d = firstReadDwsTable("hospital_review_1d", date)
    dispose(review1d)
  }

  def days_start(): Unit = {
    val review1d = daysReadDwsTable("hospital_review_1d", date)

    val reviewNd = readTable(DWS, "hospital_review_nd")
      .filter($"dt" === date_add(lit(date), -1))

    val combinedDF = review1d.union(reviewNd)
    dispose(combinedDF)
  }

  def dispose(reviewDF: DataFrame): Unit = {

    val resultDF = reviewDF.groupBy("id", "name", "street")
      .agg(
        sum("satisfaction") as "satisfaction",
        sum("ordinary") as "ordinary",
        sum("discontent") as "discontent"
      )
      .withColumn("dt", lit(date))

    writerDws(resultDF, "hospital_review_nd")

    resultDF.show()
  }

}
