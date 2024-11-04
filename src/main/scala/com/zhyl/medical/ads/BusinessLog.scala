package com.zhyl.medical.ads

import spark.implicits._
import operateHive._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.DataFrame

class BusinessLog {
  private val le: DataFrame = readTable("dwd", "live_events")

  def start(): Unit = {
    liveEvent()
  }

  def liveEvent(): Unit = {
    val inDF = le.select($"user_id", $"live_id", $"in_datetime" as "event_time", $"1" as "user_change")
    val outDF = le.select($"user_id", $"live_id", $"out_datetime" as "event_time", $"-1" as "user_change")

    val result = inDF.union(outDF)
      .withColumn("user_count", sum($"user_change") over partitionBy($"live_id").orderBy("event_time"))
      .select("user_id", "live_id", "user_count")

    result.show()

    writerAds(result, "user_count")
  }

  def pageViewEvent(): Unit = {
    readTable("dwd", "page_view_events")
      .withColumn("lagts", lag($"view_timestamp", 1, 0) over partitionBy($"user_id").orderBy($"view_timestamp"))
      .withColumn("session_start_point", when(datediff($"view_timestamp", $"lagts") >= 60, 1).otherwise(0))
      .withColumn("session_id", concat($"user_id", lit("-", sum($"session_start_point"))))


  }



}
