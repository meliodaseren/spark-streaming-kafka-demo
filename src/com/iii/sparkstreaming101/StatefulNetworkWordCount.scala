package com.iii.sparkstreaming101

import org.apache.spark._
import org.apache.spark.streaming._

object StatefulNetworkWordCount {

  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(5))

    // checkpoint directory for word states
    ssc.checkpoint("hdfs://localhost/user/cloudera/sparkstreaming101/checkpoint")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // for accumulated word counts
    def updateFunc(values: Seq[Int], state: Option[Int]): Option[Int] = {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val globalWordCounts = wordCounts.updateStateByKey(updateFunc)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    globalWordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}