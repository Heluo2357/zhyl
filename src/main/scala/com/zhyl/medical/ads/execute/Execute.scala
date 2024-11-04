package com.zhyl.medical.ads.execute

import com.zhyl.medical.ads._

object Execute {
  def main(args: Array[String]): Unit = {
    val date = args(0)

    /*val consultationCount = new ConsultationCount(date)
    consultationCount.monthConsultationSum()
    consultationCount.gradesConsultationSum()
    consultationCount.ageGradesConsultationSum()*/


    val hospitalCount = new HospitalCount(date)
    hospitalCount.streetSum()
    hospitalCount.hospitalConsultationSum()
//    hospitalCount.allHospitalReviewSum()
//    hospitalCount.hospitalReviewSum()

   /* val inquiryCount = new InquiryCount(date)
    inquiryCount.consultationSum()
    inquiryCount.drugClassInfoSum()
    inquiryCount.departmentInquirySum()
    inquiryCount.drug_season()

    val synthesize = new Synthesize(date)
    synthesize.start()*/

//    val businessLog = new BusinessLog()
//    businessLog.start()

    spark.stop()
  }
}
