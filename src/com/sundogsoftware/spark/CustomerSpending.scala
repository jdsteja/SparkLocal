package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerSpending {
  
  def extracttwofields(line: String) = {
    val fields = line.split(",")
    val ID = fields(0)
    val Spent = fields(2)
    (ID.toInt, Spent.toFloat)
    
  }
  
    def main(args: Array[String]) {

       Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]", "CustomerSpending")
       val lines = sc.textFile("../customer-orders.csv")
       val parsedLines = lines.map(extracttwofields)
       val reducedFields = parsedLines.reduceByKey((x,y)=>x+y)
       val flippedFields = reducedFields.map(x => (x._2,x._1)).sortByKey()   
       val flipagain = flippedFields.map( x => (x._2,x._1))
       val results = flipagain.collect()
       results.foreach(println)  
      
      
    }
  
  
}