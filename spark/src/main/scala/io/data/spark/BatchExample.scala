package io.data.spark

import scala.io.StdIn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import functions._
import types._

object BatchExample extends App {
  val spark = SparkSession
    .builder()
    .appName("spark-sample")
    .master("local[*]")
    .getOrCreate()

  val bankData = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ";")
    .csv(
      getClass
        .getResource("/bank.csv")
        .getPath
    )

  bankData.printSchema()

  bankData.show(10)

  StdIn.readLine()
  spark.stop()
}
