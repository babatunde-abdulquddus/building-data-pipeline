package io.data.spark

import scala.concurrent.duration._
import scala.io.StdIn

import org.apache.spark.sql.Encoders
import org.apache.spark.sql._
import org.apache.spark._
import sql.cassandra._
import sql.functions._
import sql.streaming.Trigger
import sql.types._

import com.datastax.spark._
import connector.cql.{
  CassandraConnectorConf, DefaultAuthConfFactory
}
import connector.rdd.ReadConf.SplitSizeInMBParam
import CassandraConnectorConf._
import DefaultAuthConfFactory._

object IoTStreamingExample extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("streaming-iot")
    .getOrCreate()

  val cassyOptions = {
    ConnectionHostParam.option("127.0.0.1") ++
      ConnectionPortParam.option("9042") ++
      UserNameParam.option(Some("cassandra")) ++
      PasswordParam.option(Some("cassandra")) ++
      SplitSizeInMBParam.option(16)
  }

  spark.setCassandraConf(
    cluster = "Test Cluster",
    keyspace = "analytics",
    options = cassyOptions
  )

  val to_sensor_record = udf((value: String) => {
    SensorRecord.apply(value)
  })

  val kafkaStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "iot-sensor")
    .option("startingOffsets","earliest")
    .option("kafkaConsumer.pollTimeoutMs", "256")
    .load()

  kafkaStream.printSchema()

  val stringEncoder = Encoders.STRING
  val sensorRecordEncoder = Encoders.product[SensorRecord]

  val sensorStream: Dataset[SensorRecord] = kafkaStream
    .select(col("value"))
    .as[String](stringEncoder)
    .map(SensorRecord.apply)(sensorRecordEncoder)

  sensorStream.printSchema()

    val resultStream = sensorStream
      .withColumn(
        "event_time",
        from_unixtime(col("timestamp"))
          .cast(TimestampType)
      )
      .withWatermark(
        "event_time",
        "5 minutes"
      )
      .groupBy(
        col("sensor_uuid"),
        window(
          timeColumn = col("event_time"),
          windowDuration = "15 minutes",
          slideDuration  = "5 minutes"
       )
      )
      .agg(
        avg("ambient_temperature") alias "temp_avg",
        expr("avg(humidity)") name "humidity_avg",
        max("radiation_level") as "max_radiation_level"
      )

  val streamingQuery = resultStream
    .writeStream
    .queryName("iot-sensor-agg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(3.minutes))
    .foreachBatch {
      (batch: DataFrame, batchId: Long) =>
        batch
          .select(
            col("sensor_uuid"),
            col("window.start") name "start_time",
            col("window.end") name "end_time",
            col("temp_avg"),
            col("humidity_avg"),
            col("max_radiation_level")
          )
          .orderBy(asc("sensor_uuid"))
          .write
          .mode(SaveMode.Append)
          .cassandraFormat(
            table = "sensor_averages",
            keyspace = "analytics"
          )
          .save()
    }
    .start()

  streamingQuery.awaitTermination()

  StdIn.readLine()
  streamingQuery.stop()
}
