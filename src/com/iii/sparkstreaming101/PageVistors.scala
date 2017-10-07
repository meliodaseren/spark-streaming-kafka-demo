package com.iii.sparkstreaming101

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object PageVistors {

  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(10))

    val pageViewStream = KafkaUtils.createStream(ssc,
                                                 "localhost:2182",
                                                 "consumer-group",
                                                 Map(("page_views_logs_stream",1)))
                                                 
    val pageViewStream30s = pageViewStream.window(Seconds(10),Seconds(30))
    
    
    
    val pageUser = pageViewStream.map(record => {val cols = record._2.split("\t"); (cols(3),cols(1))})
   
    
    def calculateStats(iter: Iterable[String]) = {
      var totalVistors = 0
      val uniqueDevices = collection.mutable.Set[String]()
      
      for (u <- iter) {
        totalVistors += 1
        uniqueDevices += u
      }
      (totalVistors, uniqueDevices.size)
    }
    
    
    val result = pageUser.groupByKey().mapValues(iter => calculateStats(iter))
   
    
    result.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

