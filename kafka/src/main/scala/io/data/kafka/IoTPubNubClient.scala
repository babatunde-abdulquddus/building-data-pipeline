package io.data.kafka


import scala.io.StdIn

object IoTPubNubClient extends IPubNubClient with  App {

  override def topicName: String = "iot-sensor"

  subscribe("pubnub-sensor-network")

  StdIn.readLine()
}
