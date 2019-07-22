package io.data.kafka

import java.util.Properties

import scala.collection.JavaConversions._

import com.pubnub._
import api.{PNConfiguration, PubNub}
import api.callbacks.SubscribeCallback
import api.models.consumer.PNStatus
import api.models.consumer.pubsub.{
  PNMessageResult, PNPresenceEventResult
}

import com.typesafe.scalalogging.LazyLogging

import org.apache.kafka._

import clients.producer.{
  Callback, KafkaProducer, ProducerConfig,
  ProducerRecord, RecordMetadata
}
import common.serialization.StringSerializer

trait IPubNubClient extends LazyLogging {

  def topicName: String

  //PubNub config
  val pnConfig = new PNConfiguration
  pnConfig.setSubscribeKey(
    "sub-c-5f1b7c8e-fbee-11e3-aa40-02ee2ddab7fe"
  )
  pnConfig.setSecure(false)

  val pubNub = new PubNub(pnConfig)
  pubNub.addListener(new SubscribeCallback {
    override def status(
      pubnub: PubNub,
      status: PNStatus
    ): Unit = {
      logger.info(s"Status: ${status.getCategory}")
    }

    override def message(
      pubnub: PubNub,
      message: PNMessageResult
    ): Unit = {
      writeToKafka(message.getMessage.toString)
    }

    override def presence(
      pubnub: PubNub,
      presence: PNPresenceEventResult
    ): Unit = {
      logger.info(s"Presence: ${presence.getUserMetadata}")
    }
  })

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

  val producer = new KafkaProducer[String, String](props)

  def writeToKafka(event: String): Unit = {
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
    producer.send(
      new ProducerRecord[String, String](topicName, event),
      callback
    )
  }

  def subscribe(channels: String*): Unit = {
    pubNub
      .subscribe()
      .channels(channels)
      .execute()
  }
}
