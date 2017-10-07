package com.iii.sparkstreaming101

import org.apache.spark._
import org.apache.spark.streaming._

object WindowNetworkWordCount {

  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    
    // Process last 20 seconds of data, every 5 seconds
    val linesWindow = lines.window(Seconds(20), Seconds(5))

    // Split each line into words
    val words = linesWindow.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

