package io.data.kafka

import java.util

import org.apache.kafka.common.serialization.Deserializer

import spray.json._

import JsonImplicits._

class StockPriceDeserializer extends Deserializer[StockPrice] {
  override def configure(
    configs: util.Map[String, _],
    isKey: Boolean
  ): Unit = ()

  override def deserialize(
    topic: String,
    data: Array[Byte]
  ): StockPrice = {
    val record = new String(data)
    val stockprice = record.parseJson.convertTo[StockPrice]
    stockprice
  }

  override def close(): Unit = ()
}

