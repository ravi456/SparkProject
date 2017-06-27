package com.demo.project.spark

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Created by lravikum on 20/06/17.
 * problem statement:
 * Fetch total revenue orders per day.
 * Filter orders for COMPLETE and CLOSED
 */

object TotalRevenueOrdersPerDay {
  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load()
    
    val conf = new SparkConf()
      .setAppName("Total Revenue Orders PerDay")
      .setMaster(props.getConfig(args(0)).getString("deploymentMaster"))

    val sc = new SparkContext(conf)

    val inputPath = args(1)                                  // args(1)    ---->Input Path   

    val outputPath = args(2)                                //  args(2)   ---->order_items"

    val output = new Path(outputPath)

    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    if (!fs.exists(new Path(inputPath)))                       // Input File not exist
      println("Invalid input path")

    if (fs.exists(output))                                     // Output Path exist
      fs.delete(output, true)

    val orders = sc.textFile(inputPath + "/orders").           //Parse Orders data and filter based on status
      map(rec => {
        val r = rec.split(",")
        Orders(r(0).toInt, r(1), r(2).toInt, r(3))
      }).
      filter(rec => rec.order_status == "COMPLETE" || rec.order_status == "CLOSED").
      map(rec => (rec.order_id, rec.order_date))

    val orderItems = sc.textFile(inputPath + "/order_items").    // Parse Order_items data and order_id and order_item_subtotal
      map(rec => {
        val r = rec.split(",")
        OrderItems(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4).toFloat, r(5).toFloat)
      }).
      map(rec => (rec.order_item_order_id, rec.order_item_subtotal))

    orders.join(orderItems).                                     // Join order and order_items tables and sum order_item_subtotals
      map(rec => rec._2).
      reduceByKey(_ + _).
      sortByKey().
      map(rec => rec.productIterator.mkString("\t")).
      saveAsTextFile(outputPath)

  }
}