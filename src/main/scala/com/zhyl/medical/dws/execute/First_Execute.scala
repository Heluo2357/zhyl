package com.zhyl.medical.dws.execute

import com.zhyl.medical.dws._

object First_Execute {
  def main(args: Array[String]): Unit = {
//    val date = args(0)
val date = "2024-10-21"

//    val consultPatientInfo = new ConsultPatientInfo(date)
//    consultPatientInfo.first_start()
//
//    val review1d = new HospitalReview1d(date)
//    review1d.first_start()
//
//    val reviewNd = new HospitalReviewNd(date)
//    reviewNd.first_start()

    val console1d = new HospitalConsultation1d(date)
    console1d.first_start()

    val consoleNd = new HospitalConsultationNd(date)
    consoleNd.first_start()

//
//    val drug1d = new DrugClassInfo1d(date)
//    drug1d.first_start()
//
//    val drugNd = new DrugClassInfoNd(date)
//    drugNd.first_start()
//
//
//    val registration = new DepartmentRegistration(date)
//    registration.first_start()
//
//    val registrationSuc = new DepartmentRegistrationSuc(date)
//    registrationSuc.first_start()
//
//    val drugYearMonth = new DrugYearMonth(date)
//    drugYearMonth.first_start()

    spark.stop()

  }


}
