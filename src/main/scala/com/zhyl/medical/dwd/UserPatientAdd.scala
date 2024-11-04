package com.zhyl.medical.dwd

import operateHive._
import operateTable._
import spark.implicits._
import org.apache.spark.sql.DataFrame


class UserPatientAdd {
  def first_start(date:String): Unit ={
    val patientDF = firstReadOdsTable("patient_inc", date)
    dispose(patientDF, date)
  }

  def days_start(date:String): Unit ={
    val patientDF = daysReadOdsTable("patient_inc", date)
    dispose(patientDF,date)
  }

  def dispose(patientDF: DataFrame,date:String): Unit = {
    val df = patientDF.select($"data.id",
      $"data.create_time" as "add_time",
      $"data.birthday",
      $"data.gender" as "gender_code",
      $"data.name",
      $"data.user_id"
    )

   val dictMatching= myMatching(relation(date))
   val userDF = df.withColumn("gender", dictMatching($"gender_code"))
    writerDwd(userDF,"user_patient_add_inc","add_time")

    userDF.show

  }
}

