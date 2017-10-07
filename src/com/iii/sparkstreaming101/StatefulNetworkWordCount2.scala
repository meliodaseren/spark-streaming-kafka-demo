package com.iii.sparkstreaming101

import org.apache.spark._
import org.apache.spark.streaming._

object StatefulNetworkWordCount2 {

  case class WordCountGlobal(val count: Int);

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

    // Prepare StateSpec for visitation history per IP
    // timeout(Duration idleDuration): Set the duration after which the state of an idle key will be removed.
    def trackStateFunc(batchTime: Time, word: String, count: Option[Int], state: State[WordCountGlobal]): Option[(String, WordCountGlobal)] = {
      // get previous state
      val previousState = state.getOption.getOrElse(WordCountGlobal(0))
      // update state with new data
      val newState = WordCountGlobal(previousState.count + 1)
      state.update(newState)

      Some((word, newState))
    }

    val stateSpec = StateSpec.function(trackStateFunc _)

    // Update IP visit status
    val globalWordCounts = wordCounts.mapWithState(stateSpec)

    // stateSnapshots(): Return a pair DStream where each RDD is the snapshot of the state of all the keys.
    val stateSnapshotStream = globalWordCounts.stateSnapshots()

    // word counts for this batch
    wordCounts.print()

    // running total word counts
    stateSnapshotStream.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}