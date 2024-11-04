package com.zhyl.medical.dwd.execute

import com.zhyl.medical.dwd._

object First_Execute {
  def main(args: Array[String]): Unit = {
//    val date = args(0)
    val date = "2024-10-21"
    val consul = new TradeConsultation()
    consul.first_start(date)

    val consul_pay = new TradeConsultationPaySuc()
    consul_pay.first_start(date)


    val prescription = new TradePrescription()
    prescription.first_start(date)

    val prescription_pay = new TradePrescriptionPaySuc()
    prescription_pay.first_start(date)

    val register = new DoctorRegister()
    register.first_start(date)

    val user_register = new UserRegister()
    user_register.first_start(date)

    val user_patient = new UserPatientAdd()
    user_patient.first_start(date)

    val review = new InteractionReview()
    review.first_start(date)

    spark.stop()
  }

}
