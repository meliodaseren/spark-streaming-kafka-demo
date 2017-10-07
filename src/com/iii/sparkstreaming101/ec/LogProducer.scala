package com.iii.sparkstreaming101.ec

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.kafka.clients.producer.{ KafkaProducer, Producer, ProducerConfig, ProducerRecord }
import java.util.Properties

object Config {
  val rate = 200
  val products = 200
  val referers = 20
  val pages = 2000
  val visitors = 100
  val dateFormat = "yyyy-MM-dd hh:mm:ss.S"
  val topic = "logs_stream"
}

object LogProducer extends App {

  val topic = Config.topic

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092, localhost:9093")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)

  println(kafkaProducer.partitionsFor(topic))

  val products = (0 to Config.products).map("product-" + _)
  val referers = (0 to Config.referers).map("referer-" + _)
  val pages = (0 to Config.pages).map("page-" + _)
  val visitors = (0 to Config.visitors).map("visitor-" + _)

  val rand = new scala.util.Random()

  var timeStartFrom = System.currentTimeMillis()
  var calendar = Calendar.getInstance()
  val simpleDateFormat = new SimpleDateFormat(Config.dateFormat)

  while (true) {

    for (round <- 1 to Config.rate) {

      val action = rand.nextInt(1000) % 3 match {
        case 0 => "page_view"
        case 1 => "add_to_cart"
        case 2 => "sale"
      }

      val product = products(rand.nextInt(products.length - 1))
      val referrer = referers(rand.nextInt(referers.length - 1))
      val page = pages(rand.nextInt(pages.length - 1))
      val visitor = visitors(rand.nextInt(visitors.length - 1))
      calendar.setTimeInMillis(timeStartFrom)
      val timestamp = simpleDateFormat.format(calendar.getTime())
      timeStartFrom += rand.nextInt(6000)

      val line = s"$timestamp,$referrer,$action,$visitor,$page,$product\n"

      val producerRecord = new ProducerRecord(topic, line)

      kafkaProducer.send(producerRecord)
    }

    println(s"${Config.rate} messages have been sent. Going to sleep for 1s...")
    Thread.sleep(1000)
  }
  kafkaProducer.close()
}
