package com.iii.sparkstreaming101

import org.apache.spark._
import org.apache.spark.streaming._

import java.util.regex.Pattern
import java.util.regex.Matcher

object TopURLs {

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

  def main(args: Array[String]) {

    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(1))

    val pattern = logPattern()

    val lines = ssc.socketTextStream("localhost", 9999)
    
    val requests = lines.map(line => {
      val matcher: Matcher = pattern.matcher(line)
      if (matcher.matches()) matcher.group(5) else "InvalidLogFound"
        
    })

    val urls = requests.map(x => {
      val arr = x.toString().split(" ")
      if (arr.size == 3) arr(1) else "InvalidLogFound"
    })

    val urlVisits = urls.map(x => (x, 1)).window(Seconds(600), Seconds(10)).reduceByKey(_ + _)
    
    val sorted = urlVisits.transform(rdd => rdd.sortBy(x => x._2, false))

    sorted.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}

