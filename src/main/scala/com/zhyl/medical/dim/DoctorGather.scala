package com.zhyl.medical.dim

import com.zhyl.medical.dim.spark.implicits._
import operateHive._
import org.apache.spark.sql.types.DecimalType


class DoctorGather(date:String) {
  def start(): Unit ={
    val dictDF = readOdsTable("dict_full", date)
    val doctorDF = readOdsTable("doctor_full", date)
      .select("id", "birthday", "consultation_fee", "gender", "username", "specialty", "title", "hospital_id")


    dictDF.show()
    doctorDF.show()
    val genderDic = dictDF.select($"id" as "gender_code", $"value" as "gender")
    val specialtyDic = dictDF.select($"id" as "specialty_code", $"value" as "specialty_name")
    val titleDic = dictDF.select($"id" as "title_code", $"value" as "title_name")


    val df = doctorDF.as("doctor")
      .join(genderDic as "gender", $"doctor.gender" === $"gender.gender_code", "left")
      .join(specialtyDic as "specialty", $"doctor.specialty" === $"specialty.specialty_code", "left")
      .join(titleDic as "title", $"doctor.title" === $"title.title_code", "left")
      .select($"id",
        $"birthday",
        $"consultation_fee" cast DecimalType(19, 2),
        $"gender.gender_code",
        $"gender.gender",
        $"username",
        $"specialty.specialty_code",
        $"specialty.specialty_name",
        $"title.title_code",
        $"title.title_name",
        $"hospital_id")
//
    writerDim(df, "doctor_full",date)
    df.show()
  }

}
