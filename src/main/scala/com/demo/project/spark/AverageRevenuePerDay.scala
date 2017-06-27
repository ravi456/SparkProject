package com.demo.project.spark

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Created by lravikum on 20/06/17.
 * problem statement:
 * Fetch Average revenue orders per day.
 * 
 */

object AverageRevenuePerDay {
  
   def main(args: Array[String]): Unit = {
  
   val props = ConfigFactory.load()
    
    val conf = new SparkConf()
      .setAppName("Average Revenue PerDay")
      .setMaster(props.getConfig(args(0)).getString("deploymentMaster"))

    val sc = new SparkContext(conf)

    val inputPath = args(1)                                  // args(1)    ---->Input Path   
   
    val outputPath = args(2)                                 //  args(2)    ----> Output Path

    val output = new Path(outputPath)

    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    if (!fs.exists(new Path(inputPath)))                       // Input File not exist
      println("Invalid input path")

    if (fs.exists(output))                                     // Output Path exist
      fs.delete(output, true)


    val ordersRDD = sc.textFile(inputPath + "/orders")                     
    val orderItemsRDD = sc.textFile(inputPath + "/order_items")

    val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))                        //  Parse Orders (key order_id)
    val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))                //  Parse Order items (key order_item_order_id)

    val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)                         //  Join the data sets
    
    val ordersJoinOrderItemsMap = ordersJoinOrderItems.map(t => 
       ((t._2._2.split(",")(1), t._1), t._2._1.split(",")(4).toFloat))                           //Parse joined data and get (order_date, order_id) as key  and order_item_subtotal as value

    val revenuePerDayPerOrder = ordersJoinOrderItemsMap.reduceByKey((acc, value) => acc + value)  // Use appropriate aggregate function to get sum(order_item_subtotal) for each order_date, order_id combination  
    
    val revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(rec => (rec._1._1, rec._2))          // Parse data to discard order_id and get order_date as key and sum(order_item_subtotal) per order as value

    val revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey((0.0, 0))(         
      (acc, revenue) => (acc._1 + revenue, acc._2 + 1),                                            // Use appropriate aggregate function to get sum(order_item_subtotal) per day and count(distinct order_id) per day
      (total1, total2) => (total1._1 + total2._1, total1._2 + total2._2))

    revenuePerDay.collect().foreach(println)

    val avgRevenuePerDay = revenuePerDay.map(x => (x._1, x._2._1 / x._2._2))
    
    avgRevenuePerDay.collect().foreach(println)
    
    val avgRevenueData = avgRevenuePerDay.sortByKey()
     
     avgRevenueData.saveAsTextFile(outputPath)
      
   }
  
}