package io.data.kafka

import scala.io.StdIn

class StockPubNubClient extends IPubNubClient  with App {
  override def topicName: String = "market-orders"

  subscribe("pubnub-market-orders")

  StdIn.readLine()
}
