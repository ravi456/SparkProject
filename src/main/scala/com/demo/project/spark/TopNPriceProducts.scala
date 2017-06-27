package com.demo.project.spark

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Created by lravikum on 20/06/17.
 * problem statement:
 * Fetch Top N price products list.
 *
 */

object TopNPriceProducts {

  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load()

    val conf = new SparkConf()
      .setAppName("Top N Priced products")
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
      
     def topNPricedProducts(rec: (String, Iterable[String]), topN: Int): Iterable[(String, String)] = {
      val list = rec._2.toList
      val topNPrices = list.
        map(rec => rec.split(",")(4).toFloat).
        sortBy(k => -k).distinct.take(topN)
      val sortedList = list.sortBy(k => -k.split(",")(4).toFloat)
      sortedList.
        filter(r => topNPrices.contains(r.split(",")(4).toFloat)).
        map(r => (rec._1, r))

    }

    val products = sc.textFile(inputPath + "/products")                          // Pass argument as product file
    val productsFiltered = products.filter(rec => rec.split(",")(4) != "")
    val productsMap = productsFiltered.map(rec => (rec.split(",")(1).toInt, rec))

    val categories = sc.textFile(inputPath + "/categories").                    // Pass argument as categories file
      map(rec => (rec.split(",")(0).toInt, rec.split(",")(2)))

    val productsJoin = productsMap.
      join(categories).
      map(rec => (rec._2._2, rec._2._1))
    val productsGroupByCategory = productsJoin.groupByKey()
    productsGroupByCategory.
      flatMap(rec => topNPricedProducts(rec, 1)).
      collect().
      foreach(println)  
      
     val productsGroupByCategories = productsGroupByCategory.sortByKey()
     
     productsGroupByCategories.saveAsTextFile(outputPath)

  }

}