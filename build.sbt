val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.12",
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:implicitConversions"
  ),
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core"    % "jackson-core"          % "2.9.9",
    "com.fasterxml.jackson.core"    % "jackson-databind"      % "2.9.9",
    "com.fasterxml.jackson.module" %% "jackson-module-scala"  % "2.9.9"
  ),
  resolvers ++= Seq(
    "Confluent Maven Repository"             at "http://packages.confluent.io/maven/",
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    "Spark Packages Repo"                    at "https://dl.bintray.com/spark-packages/maven"
  )
)

val sparkVersion = "2.4.3"

lazy val root = (project in file(".")).aggregate(kafka, spark)

lazy val kafka = (project in file("kafka")).settings(
  name := "working-with-kafka",
  commonSettings,
  libraryDependencies ++= Seq(
    "org.apache.kafka"            % "kafka-streams" % "2.3.0",
    "org.apache.kafka"            % "kafka-clients" % "2.3.0",
    "com.pubnub"                  % "pubnub-gson"   % "4.25.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )
)

lazy val spark = (project in file("spark"))
  .settings(
    name := "working-with-spark",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark"   %% "spark-core"                      % sparkVersion % Compile,
      "org.apache.spark"   %% "spark-sql"                       % sparkVersion % Compile,
      "org.apache.spark"   %% "spark-streaming"                 % sparkVersion % Compile,
      "org.apache.spark"   %% "spark-avro"                      % sparkVersion % Compile,
      "org.apache.spark"   %  "spark-streaming-kafka-0-10_2.11" % sparkVersion % Compile,
      "org.apache.spark"   %  "spark-sql-kafka-0-10_2.11"       % sparkVersion % Compile,
      "com.datastax.spark" %% "spark-cassandra-connector"       % "2.4.1"      % Compile
    )
  )

