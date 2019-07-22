package io.data

import spray.json._

package object spark {
  case class SensorRecord(
    sensor_uuid: String,
    photosensor: Double,
    radiation_level: Double,
    humidity: Double,
    ambient_temperature: Double,
    timestamp: Long
  )

  implicit object SensorRecordFormat
    extends RootJsonFormat[SensorRecord] {
    override def write(obj: SensorRecord): JsValue = {
      JsObject(
        "sensor_uuid" ->
          JsString(obj.sensor_uuid),
        "photosensor"
          -> JsString(obj.photosensor.toString),
        "radiation_level" ->
          JsString(obj.radiation_level.toString),
        "humidity"
          -> JsString(obj.humidity.toString),
        "ambient_temperature"
          -> JsString(obj.ambient_temperature.toString),
        "timestamp"
          -> JsNumber(obj.timestamp)
      )
    }

    override def read(value: JsValue): SensorRecord =
      value.asJsObject.getFields(
        "photosensor",
        "radiation_level",
        "humidity",
        "ambient_temperature",
        "sensor_uuid", "timestamp"
      ) match {
        case Seq(
          JsString(photoSensor),
          JsString(radiationLevel),
          JsString(humidity),
          JsString(temperature),
          JsString(sensorUUID),
          JsNumber(timestamp)
        ) =>
          new SensorRecord(
            sensor_uuid = sensorUUID,
            photosensor = photoSensor.toDouble,
            radiation_level = radiationLevel.toDouble,
            humidity = humidity.toDouble,
            ambient_temperature = temperature.toDouble,
            timestamp = timestamp.toLongExact
          )
        case _ =>
          deserializationError(
            "Json object couldn't be converted " +
              " to SensorRecord object"
          )
    }
  }

  object SensorRecord {
    def apply(jsonString: String): SensorRecord = {
       jsonString.parseJson.convertTo[SensorRecord]
    }
  }
}