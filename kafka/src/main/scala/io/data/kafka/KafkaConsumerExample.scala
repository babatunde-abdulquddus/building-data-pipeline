package io.data.kafka

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import scala.io.StdIn
import org.apache.kafka._
import clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition

object KafkaConsumerExample extends App with LazyLogging {

  logger.info("Polling for events from stock-prices topic")


  while(true) {

    val firstConsumer = new StockPriceConsumer() {
      override val logger =  Logger("io.data.kafka.StockPriceConsumer-1")
    }

    val secondConsumer = new StockPriceConsumer() {
      override val logger =  Logger("io.data.kafka.StockPriceConsumer-2")
    }

    firstConsumer.consumer.subscribe(List("stock-prices"))
    //firstConsumer.consumer.assign(List(new TopicPartition("stock-prices", 0)))
    //secondConsumer.consumer.assign(List(new TopicPartition("stock-prices", 1)))

    val firstRecords: ConsumerRecords[String, StockPrice] = firstConsumer.consumer.poll(java.time.Duration.ofMillis(1000))

  // val secondRecords: ConsumerRecords[String, StockPrice] = secondConsumer.consumer.poll(java.time.Duration.ofMillis(1000))

    if(firstRecords.count() != 0) {
      firstRecords.foreach{ record =>
        firstConsumer.logger.info(s"Consumed StockPrice Record: ${record.value()} in partition ${record.partition()} ")
      }
    }
    Thread.sleep(100)
/*
    if(secondRecords.count() != 0) {
      firstRecords.foreach{ record =>
        secondConsumer.logger.info(s"Consumed StockPrice Record: ${record.value()} in partition ${record.partition()} ")
      }
    }
*/
    firstConsumer.consumer.close()
    //secondConsumer.consumer.close()
    StdIn.readLine()
 }
}

trait StockPriceConsumer {
  val logger: Logger
  // Kafka consumer config
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StockPriceDeserializer])
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-prices-consumer-group")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  // props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50")

  val consumer = new KafkaConsumer[String, StockPrice](props)
}


