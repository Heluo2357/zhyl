package com.zhyl.medical.dwd.execute

import com.zhyl.medical.dwd._

object Days_Execute {
  def main(args: Array[String]): Unit = {
    val date = args(0)

    val consul = new TradeConsultation()
    consul.days_start(date)

    val consul_pay = new TradeConsultationPaySuc()
    consul_pay.days_start(date)

    val prescription = new TradePrescription()
    prescription.days_start(date)

    val prescription_pay = new TradePrescriptionPaySuc()
    prescription_pay.days_start(date)

    val register = new DoctorRegister()
    register.days_start(date)

    val user_register = new UserRegister()
    user_register.days_start(date)

    val user_patient = new UserPatientAdd()
    user_patient.days_start(date)

    val review = new InteractionReview()
    review.days_start(date)

     spark.stop()


  }

}
