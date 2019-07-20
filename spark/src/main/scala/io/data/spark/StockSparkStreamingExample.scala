package io.data.spark

import scala.concurrent.duration._

import com.datastax.spark.connector.cql._

import CassandraConnectorConf._

import DefaultAuthConfFactory._

import com.datastax.spark._


import connector.rdd.ReadConf.SplitSizeInMBParam

import org.apache.spark.sql._

import cassandra._
import functions._
import streaming.Trigger
import types._

import scala.io.StdIn

class StockSparkStreamingExample {

  /***
    * {
    * bid_price:212.6808,
    * order_quantity:739,
    * symbol:'Bespin Gas',
    * timestamp:12342343234,
    * trade_type:"limit"
    * }
    *
    *
    */

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("streaming-stocks")
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


  val kafkaStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "market-orders")
    .option("startingOffsets","earliest")
    .option("kafkaConsumer.pollTimeoutMs", "256")
    // .option("checkpointLocation", "/tmp/checkpoint")
    //  .option("failOnDataLoss", "true")
    //   .option("fetchOffset.numRetries", "3")
    //   .option("fetchOffset.retryIntervalMs", "10")
    //   .option("maxOffsetsPerTrigger", "1000")
    .load()

  kafkaStream.printSchema()


  // Defining market orders schema approach
  val schema = StructType(
    Array(
      StructField("bid_price", DoubleType, nullable = false),
      StructField("order_quantity", LongType, nullable = false),
      StructField("symbol", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("trade_type", StringType, nullable = false)
    )
  )

  val marketOrdersStream = kafkaStream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value") as "market_orders_records", schema))
    .select("market_orders_records.*")

  marketOrdersStream.printSchema()

  val resultStream = marketOrdersStream
    .withColumn("event_time", from_unixtime(col("timestamp")))
    .groupBy(
      col("symbol"),
      col("trade_type"),
      window(
        timeColumn = col("event_time"),
        windowDuration = "15 minutes",
        slideDuration = "5 minutes"
      )
    )
    .agg(
      avg("bid_price") alias "avg_bid_price",
      max("bid_price") alias "max_bid_price",
      min("bid_price") alias "min_bid_price",
      avg("order_quantity") alias "avg_order_qty",
      max("order_quantity") alias "max_order_qty",
      min("order_quantity") alias "min_order_qty"
    )


  val streamingQuery = resultStream
    .writeStream
    .queryName("market-orders-agg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(3 minutes))
    .foreachBatch {
      (batch: DataFrame, batchId: Long) =>
        batch
          .select(
            col("symbol"),
            col("window.start") name "start_time",
            col("window.end") name "end_time",
            col("avg_bid_price"),
            col("max_bid_price"),
            col("min_bid_price"),
            col("avg_order_qty"),
            col("max_order_qty"),
            col("min_order_qty")
          )
          .orderBy(asc("symbol"))
          .write
          .mode(SaveMode.Append)
          .cassandraFormat(table = "market_order_agg", keyspace = "analytics")
          .save()
    }
    .start()


  streamingQuery.awaitTermination()

  StdIn.readLine()
/*
  val socketStream = spark
    .readStream
    .format("socket")
    .options(Map[String, String]("host" -> "localhost", "port" -> "9786", "includeTimestamp" -> "true"))
    .load()

  val sampleQuery = socketStream
    .writeStream
    .format("memory")
    .queryName("memory-sink")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(2 minutes))
    .start()
    */
}
