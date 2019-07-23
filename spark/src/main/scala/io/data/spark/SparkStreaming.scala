package io.data.spark

import scala.io.StdIn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

import functions._

object SparkStreaming extends App {
  val spark = SparkSession
    .builder()
    .appName("spark-streaming-sample")
    .master("local[*]")
    .getOrCreate()

  val socketStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val resultStream = socketStream
    .withWatermark("timestamp", "5 minutes")
    .groupBy(col("sensor_id"), window(col("timestamp"), "15 minutes"))
    .agg(avg("temperature") alias "temp_avg")
    .agg(avg("humidity") alias "humidity_avg")


  val streamingQuery = resultStream
    .writeStream
    .format("memory")
    .outputMode("update")
    .option("checkPointLocation", "")
    .start()

  streamingQuery.awaitTermination()

  StdIn.readLine()
}
