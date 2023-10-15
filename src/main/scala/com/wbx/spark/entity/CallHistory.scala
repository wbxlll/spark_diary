package com.wbx.spark.entity

//话单对象
case class CallHistory(
                    impiFrom:String,
                    impiTo:String,
                    callTime: Long,
                    callDuration:Int,
                    impiFromLocation:String
                  )

