package com.demo.project.spark
 import scala.io.Source 

 case class Transactions(
     
     transactionId: String,
     accountId: String,
     transactionDay: Int, 
     category: String,
     transactionAmount: Double)