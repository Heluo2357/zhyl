package com.zhyl.medical.dws.execute

import com.zhyl.medical.dws._

object Days_Execute {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    /*val consultPatientInfo = new ConsultPatientInfo(date)
    consultPatientInfo.days_start()

    val review1d = new HospitalReview1d(date)
    review1d.days_start()

    val reviewNd = new HospitalReviewNd(date)
    reviewNd.days_start()*/

    val console1d = new HospitalConsultation1d(date)
    console1d.days_start()

    val consoleNd = new HospitalConsultationNd(date)
    consoleNd.days_start()

    /*val drug1d = new DrugClassInfo1d(date)
    drug1d.days_start()

    val drugNd = new DrugClassInfoNd(date)
    drugNd.days_start()
*/
    /*val registration = new DepartmentRegistration(date)
    registration.days_start()


    val registrationSuc = new DepartmentRegistrationSuc(date)
    registrationSuc.days_start()

    val drugYearMonth = new DrugYearMonth(date)
    drugYearMonth.days_start()*/

    spark.stop()
  }
}
