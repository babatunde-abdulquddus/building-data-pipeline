package io.data.kafka

import java.io.File
import java.util.Properties

import scala.io.{Source, StdIn}

import com.typesafe.scalalogging.LazyLogging

import org.apache.kafka._
import clients.producer.{
  Callback, KafkaProducer, ProducerConfig,
  ProducerRecord, RecordMetadata
}
import common.serialization.StringSerializer

import spray.json._

object KafkaProducerExample extends LazyLogging {
  // Kafka producer config
  val props = new Properties()

  props.put(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "localhost:29092"
  )
  props.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )
  props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )
  props.put(
    ProducerConfig.CLIENT_ID_CONFIG,
    "stock-prices-publisher"
  )

  val producer =
    new KafkaProducer[String, String](props)
  val topicName = "stock-prices"

  val (apple, microsoft) = ("AAPL", "MSFT")


  def main(args: Array[String]): Unit = {

    val source = Source.fromFile(
      file = new File(
        getClass.getResource("/msft_aapl.csv")
          .getPath
      )
    )

    val stockPrices: Seq[StockPrice] =  source
      .getLines()
      .drop(1)
      .map(line => line.split(",").map(_.trim))
      .flatMap { row =>
        List(
          StockPrice(
            date = row(0),
            symbol = apple,
            price = row(1).toDouble
          ),
          StockPrice(
            date = row(0),
            symbol = microsoft,
            price = row(2).toDouble
          )
        )
      }
      .toList

    import JsonImplicits._

    stockPrices.foreach{ stockPrice =>
      writeToKafka(
        stockPrice.symbol,
        stockPrice.toJson.compactPrint
      )
    }

    StdIn.readLine()
  }

  def writeToKafka(
    key: String,
    event: String
  ): Unit = {
    val producerRecord =
      if(key == apple)
        new ProducerRecord[String, String](
          "stock-prices",
          0,
          key,
          event
        )
      else
        new ProducerRecord[String, String](
          "stock-prices",
          1,
          key,
          event
        )

    val callback = new Callback {
      override def onCompletion(
        metadata: RecordMetadata,
        exception: Exception
      ): Unit = {
        if(exception == null)
          logger.info("Successful write")
        else
          logger.warn(s"Failed write: $exception")
      }
    }

    producer.send(producerRecord,callback)
  }
}
