package com.zhyl.medical.dim

import com.zhyl.medical.dim.spark.implicits._
import operateHive._

class PatientGather(date:String) {

  def start(): Unit ={
    val dictDF = readOdsTable("dict_full",date)

    val patient = readOdsTable("patient_full",date)

    val genderDic = dictDF.select($"id" as "gender_code", $"value" as "gender")

    val df = patient.as("patientDF")
      .join(genderDic as "genderDF", $"patientDF.gender" === $"genderDF.gender_code")
      .select($"id", $"birthday", $"genderDF.gender_code", $"genderDF.gender", $"name", $"user_id")

    writerDim(df, "patient_full",date)

  }

}
