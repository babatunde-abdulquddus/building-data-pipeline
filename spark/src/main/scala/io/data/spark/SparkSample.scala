package io.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import functions._
import types._

object SparkSample extends App {
  val spark = SparkSession
    .builder()
    .appName("spark-sample")
    .master("local[8]")
    .getOrCreate()

  val iotSample= spark
    .read
    .option("inferSchema", "true")
    .json(getClass.getResource("/iot-sample.json").getPath)

  iotSample.printSchema()

  iotSample
    .withColumn("timestamp", col("timestamp").cast(TimestampType))
    .show()

  iotSample
    .groupBy(col("sensor_uuid"))
    .agg(
      expr("avg(ambient_temperature)") alias "temp_avg",
      expr("avg(humidity)") alias "humidity_avg",
      expr("avg(radiation_level)") alias "radiation_avg"
    )
    .show(30)

  spark.stop()
}
