package io.data.kafka

import java.time.Duration
import java.util.Properties

import scala.collection.JavaConversions._
import scala.io.StdIn

import com.typesafe.scalalogging.{LazyLogging, Logger}

import org.apache.kafka._
import clients.consumer.{ConsumerConfig, KafkaConsumer}
import common.TopicPartition
import common.serialization.StringDeserializer

object KafkaConsumerExample extends App
  with LazyLogging {

  logger.info(
    "Polling for events from stock-prices topic"
  )

  val topicName = "stock-prices"

  while(true) {

    val firstConsumer = new StockPriceConsumer() {
      override val logger =
        Logger("io.data.kafka.StockPriceConsumer-1")
    }

    val secondConsumer = new StockPriceConsumer() {
      override val logger =
        Logger("io.data.kafka.StockPriceConsumer-2")
    }

    firstConsumer.consumer.assign(
      List(
        new TopicPartition(
          topicName,
          0
        )
      )
    )

    secondConsumer.consumer.assign(
      List(
        new TopicPartition(
          topicName,
          1
        )
      )
    )

    val firstConsumerRecords =
      firstConsumer
        .consumer
        .poll(Duration.ofMillis(1000))

   val secondConsumerRecords =
     secondConsumer
       .consumer
       .poll(Duration.ofMillis(1000))

    if(firstConsumerRecords.count() != 0) {
      firstConsumerRecords.foreach{ record =>
        firstConsumer.logger.info(
          s"Consumed StockPrice Record: " +
          s" ${record.value()} in partition: " +
          s" ${record.partition()} "
        )
      }
    }


    if(secondConsumerRecords.count() != 0) {
      secondConsumerRecords.foreach{ record =>
        secondConsumer.logger.info(
          s"Consumed StockPrice Record: " +
          s" ${record.value()} in partition: " +
          s" ${record.partition()} "
        )
      }
    }

    StdIn.readLine()
    firstConsumer.consumer.close()
    secondConsumer.consumer.close()
 }
}

trait StockPriceConsumer {
  val logger: Logger

  // Kafka consumer config
  val props = new Properties()

  props.put(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "localhost:29092"
  )
  props.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    classOf[StringDeserializer]
  )
  props.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    classOf[StockPriceDeserializer]
  )
  props.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    "stock-prices-consumer-group"
  )
  props.put(
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
    "false"
  )
  props.put(
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
    "earliest"
  )

  val consumer =
    new KafkaConsumer[String, StockPrice](props)
}


