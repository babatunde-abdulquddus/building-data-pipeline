package io.data.kafka

import java.util.Properties

import scala.collection.JavaConversions._
import scala.io.StdIn

import com.pubnub.api.callbacks.SubscribeCallback
import com.pubnub.api.models.consumer.pubsub.{PNMessageResult, PNPresenceEventResult}
import com.pubnub.api.models.consumer.PNStatus
import com.pubnub.api.{PNConfiguration, PubNub}

import com.typesafe.scalalogging.LazyLogging

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object PubNubClient extends App with LazyLogging {

  //PubNub config
  val pnConfig = new PNConfiguration
  pnConfig.setSubscribeKey("sub-c-5f1b7c8e-fbee-11e3-aa40-02ee2ddab7fe")
  pnConfig.setSecure(false)

  val pubNub = new PubNub(pnConfig)
  pubNub.addListener(new SubscribeCallback {
    override def status(pubnub: PubNub, status: PNStatus): Unit = {
      logger.info(s"Status: ${status.getCategory}")
    }

    override def message(pubnub: PubNub, message: PNMessageResult): Unit = {
      writeToKafka(message.getMessage.toString)
    }

    override def presence(pubnub: PubNub, presence: PNPresenceEventResult): Unit = {
      logger.info(s"Presence: ${presence.getUserMetadata}")
    }
  })

  pubNub
    .subscribe()
    .channels(List("pubnub-sensor-network"))
    .execute()

  // Kafka producer config
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  def writeToKafka(event: String): Unit = {
    producer.send(
      new ProducerRecord[String, String]("iot-sensor", event),
      new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if(exception == null) logger.info("Successful write")
          else logger.warn(s"Failed write: $exception")
        }
      }
    )
  }

  StdIn.readLine()
}
