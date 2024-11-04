package com.zhyl.medical.dim

import com.zhyl.medical.dim.spark.implicits._
import operateHive._

class UserGather(date:String) {
   def start(): Unit ={
     val df = readOdsTable("user_full",date)
       .select($"id", $"email", $"telephone", $"username")
       writerDim(df, "user_full",date)
   }
}
