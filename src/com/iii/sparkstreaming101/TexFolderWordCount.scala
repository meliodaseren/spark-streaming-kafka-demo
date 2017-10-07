package com.iii.sparkstreaming101

import org.apache.spark._
import org.apache.spark.streaming._

object TextFolderWordCount {

  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.textFileStream("hdfs://localhost/user/cloudera/spark_streaming_101/textfolder")
    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

