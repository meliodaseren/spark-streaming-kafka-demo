package com.iii.sparkstreaming101.ec

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.kafka.clients.consumer._
import java.util.Properties

object LogConsumerConfig {
  val topic = "logs_stream"
}

object LogConsumer extends App {

  val topic = LogConsumerConfig.topic
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092, localhost:9093")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor")
  props.put("group.id", "LogConsumerConfig")
  props.put("client.id", "LogConsumerConfig")

  val kafkaConsumer: KafkaConsumer[Nothing, String] = new KafkaConsumer[Nothing, String](props)

  kafkaConsumer.subscribe(LogConsumerConfig.topic)
  
  try {
    while (true) {
      val records = kafkaConsumer.poll(3000)
      println(records)
      Thread.sleep(1000)
      
    }
  } finally {
    kafkaConsumer.close()
  }
}

