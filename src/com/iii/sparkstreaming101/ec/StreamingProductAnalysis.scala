package com.iii.sparkstreaming101.ec

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD

case class LogEntry(timestamp: String,
                    referrer: String,
                    action: String,
                    visitor: String,
                    page: String,
                    product: String)

case class UserActivity(visitor: String,
                        numberOfSales: Long,
                        numberOfAddedToCarts: Long,
                        numberOfPageViews: Long,
                        numberOfEvents: Long)

object StreamingProductAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ec")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("hdfs://localhost/user/cloudera/spark_streaming_101/checkpoint")

    val activityStream = KafkaUtils.createStream(ssc, "localhost:2182", "consumer-group", Map(("logs_stream", 1)))
      .map(_._2)

    import spark.implicits._

    val userActivityStream = activityStream.transform { input =>

      val inputDF = input.flatMap { line =>
        val record = line.split(",")
        if (record.length == 6)
          Some(LogEntry(record(0), record(1), record(2), record(3), record(4), record(5)))
        else
          None
      }.toDF().cache()

      inputDF.createOrReplaceTempView("logs")

      val userActivityDF = spark.sql("""SELECT
                                    visitor,
                                    sum(case when action = 'sale' then 1 else 0 end) as number_of_sales,
                                    sum(case when action = 'add_to_cart' then 1 else 0 end) as number_of_add_to_cart,
                                    sum(case when action = 'page_view' then 1 else 0 end) as number_of_page_views,
                                    sum(1)
                                    from logs
                                    group by visitor""")

      userActivityDF.rdd.map(r => {
        r.getString(0) -> UserActivity(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
      })

    }

    userActivityStream.reduceByKeyAndWindow((userAct1: UserActivity, userAct2: UserActivity) => {
      val numberOfSales = userAct1.numberOfSales + userAct2.numberOfSales
      val numberOfAddedToCarts = userAct1.numberOfAddedToCarts + userAct2.numberOfAddedToCarts
      val numberOfPageViews = userAct1.numberOfPageViews + userAct2.numberOfPageViews
      val numberOfEvents = userAct1.numberOfEvents + userAct2.numberOfEvents

      UserActivity(userAct1.visitor, numberOfSales, numberOfAddedToCarts, numberOfPageViews, numberOfEvents)
    }, Seconds(300), Seconds(10)).print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}

