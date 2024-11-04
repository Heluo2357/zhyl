package com.zhyl.medical.dwd

import operateHive._
import operateTable._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DecimalType
import spark.implicits._

// TODO: 医生域医生注册事务事实表
class DoctorRegister {

  def first_start(date: String): Unit = {
     val register = firstReadOdsTable("doctor_inc",date)
     dispose(register,date)
  }

  def days_start(date:String): Unit ={
    val register = daysReadOdsTable("doctor_inc", date)
    dispose(register,date)

  }

  def dispose(registerDF:DataFrame,date:String): Unit ={

    val register = registerDF.select($"data.id",
      $"data.create_time" as "register_time",
      $"data.birthday",
      ($"data.consultation_fee" cast DecimalType(19, 2)) as "consultation_fee",
      $"data.gender" as "gender_code",
      $"data.username",
      $"data.specialty" as "specialty_code",
      $"data.title" as "title_code",
      $"data.hospital_id")

    val dictMatching= myMatching(relation(date))

    val df= register
      .withColumn("gender", dictMatching($"gender_code"))
      .withColumn("specialty_name", dictMatching($"specialty_code"))
      .withColumn("title_name", dictMatching($"title_code"))

    writerDwd(df, "doctor_register_inc", "register_time")
    df.show()
  }


}

