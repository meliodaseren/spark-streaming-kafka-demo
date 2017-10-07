package com.iii.sparkstreaming101

import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

object TrafficMonitor {

  def logPattern(): Pattern = {
    val threeDigits = "\\d{1,3}"
    val ip = s"($threeDigits\\.$threeDigits\\.$threeDigits\\.$threeDigits)?"
    val client = "(\\S+)"
    val uid = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val statusCode = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $uid $dateTime $request $statusCode $bytes $referer $agent"
    Pattern.compile(regex)
  }
  case class IPStatus(val visits: Long, var clickstream: List[String]);
  case class IPStatusRecord(ip: String, visits: Long, clickstream: List[String])

  def main(args: Array[String]) {
    // Initialization
    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("hdfs://localhost/user/cloudera/spark_streaming_101/checkpoint/")
    val pattern = logPattern()

    // Open socket stream
    val lines = ssc.socketTextStream("localhost", 9999).window(Seconds(5),Seconds(5))

    // Transform each log to be (ip, url) pair
    val ipUrlPairs = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = if (requestFields.size == 3) requestFields(1) else "InvalidLogFound"
        (ip, url)
      } else {
        ("InvalidLogFound", "InvalidLogFound")
      }
    })

    // Prepare StateSpec for visitation history per IP
    // timeout(Duration idleDuration): Set the duration after which the state of an idle key will be removed.
    def trackStateFunc(batchTime: Time, ip: String, url: Option[String], state: State[IPStatus]): Option[(String, IPStatus)] = {
      // get previous state
      val previousState = state.getOption.getOrElse(IPStatus(0, List()))
      // update state with new data
      val newState = IPStatus(previousState.visits + 1, (previousState.clickstream :+ url.getOrElse("empty")).take(20))
      state.update(newState)

      Some((ip, newState))
    }

    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))

    // Update IP visit status
    val ipUrlPairsWithState = ipUrlPairs.mapWithState(stateSpec)

    // stateSnapshots(): Return a pair DStream where each RDD is the snapshot of the state of all the keys.
    val stateSnapshotStream = ipUrlPairsWithState.stateSnapshots()

    // Output result using foreachRDD
    stateSnapshotStream.foreachRDD((rdd, time) => {
      val spark = SparkSession
        .builder()
        .getOrCreate()

      import spark.implicits._
      val requestsDataFrame = rdd.map(x => IPStatusRecord(x._1, x._2.visits, x._2.clickstream)).toDF()
      requestsDataFrame.createOrReplaceTempView("IPStatus")
      val sessionsDataFrame = spark.sql("select * from IPStatus order by visits desc")
      println(time)
      sessionsDataFrame.show(20)
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}