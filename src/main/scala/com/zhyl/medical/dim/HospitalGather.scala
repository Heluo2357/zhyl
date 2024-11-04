package com.zhyl.medical.dim

import operateHive._
class HospitalGather(date:String) {

  def start(): Unit ={
    val df = readOdsTable("hospital_full",date)
      .drop("create_time", "update_time")

    writerDim(df,"hospital_full",date)
  }
}
