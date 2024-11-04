package com.zhyl.medical.dim

class MedicineGather(date:String){
  def start(): Unit ={
    writeOdsToDim("medicine_full",date)
  }
}
