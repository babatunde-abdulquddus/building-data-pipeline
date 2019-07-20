package io.data

import spray.json._

import DefaultJsonProtocol._

package object kafka {
  case class StockPrice(date: String, symbol: String, price: Double)


  object JsonImplicits {
    implicit val stockPriceFormat = jsonFormat3(StockPrice)
  }
}
