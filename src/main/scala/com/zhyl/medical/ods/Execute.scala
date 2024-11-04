package com.zhyl.medical.ods


object Execute {
  def main(args: Array[String]): Unit = {
    val date = args(0)

    load_table("consultation_inc", date)
    load_table("dict_full", date)
    load_table("doctor_full", date)
    load_table("doctor_inc", date)
    load_table("hospital_full", date)
    load_table("medicine_full", date)
    load_table("patient_full", date)
    load_table("prescription_detail_inc", date)
    load_table("user_full", date)
    load_table("prescription_inc", date)
    load_table("user_inc", date)
    load_table("patient_inc", date)
    load_table("payment_inc", date)



    spark.stop()
  }
}
