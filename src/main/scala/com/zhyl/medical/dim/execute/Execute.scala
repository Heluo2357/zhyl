package com.zhyl.medical.dim.execute

import com.zhyl.medical.dim._

object Execute {
  def main(args: Array[String]): Unit = {
    val date = args(0)

    val doctor = new DoctorGather(date)
    val patient = new PatientGather(date)
    val hospital = new HospitalGather(date)
    val medicine = new MedicineGather(date)
    val user = new UserGather(date)
    doctor.start()
    patient.start()
    hospital.start()
    medicine.start()
    user.start()

    spark.stop()
  }

}
