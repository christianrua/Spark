package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._
import scala.collection.immutable.ListMap

object CustomerSpend {
  
     def parseLine(line: String) = {
        // Split by commas
        val fields = line.split(",")
        // Extract the Customer ID and amount, and convert to integers
        val IdCustomer = fields(0).toInt
        val amount = fields(2).toFloat
        // Create a tuple that is our result.
        (IdCustomer, amount)
        }
     def main(args : Array[String]): Unit = {
       
          // Set the log level to only print errors
          Logger.getLogger("org").setLevel(Level.ERROR)
    
          // Create a SparkContext using every core of the local machine
          val sc = new SparkContext("local[*]", "CustomerSpend")  

    
          // Load each line of my book into an RDD
          val input = sc.textFile("../SparkScala3/customer-orders.csv")
          
         // Split using a regular expression that extracts words
          val values = input.map(parseLine)
           
          val IdAmount = values.reduceByKey((x,y) => x + y )
          
         // Flip (Id, Spend) tuples to (Spend, Id) and then sort by key (the total spend amount)
          val IdAmountSorted1 = IdAmount.map( x => (x._2, x._1) ).sortByKey(false)
          
        
        // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
          val results = IdAmountSorted1.collect()
    
    
         results.foreach(println)
          
          
          
    
          
     }
  
   
  
}
