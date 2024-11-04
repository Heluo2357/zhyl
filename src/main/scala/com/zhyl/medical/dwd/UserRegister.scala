package com.zhyl.medical.dwd

import spark.implicits._
import operateHive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class UserRegister {
  def first_start(date: String): Unit = {
    val userDF = firstReadOdsTable("user_inc", date)
    dispose(userDF)
  }

  def days_start(date: String): Unit = {
    val userDF = daysReadOdsTable("user_inc", date)
    dispose(userDF)
  }

  def dispose(userDF: DataFrame): Unit = {
    val df = userDF.select(
      $"data.id",
      $"data.create_time" as "register_time",
      concat(lit("*@"), split($"data.email", "@")(1)).as("email"),
      when($"data.telephone".rlike("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$"),
        concat(substring($"data.telephone", 1, 3), lit("********")))
        .otherwise(null).as("telephone"), $"data.username")

    writerDwd(df,"user_register_inc","register_time")

    df.show()
  }
}
