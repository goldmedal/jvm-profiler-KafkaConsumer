package com.wasai.data.profile

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaResultConsumer {

  val conf = new SparkConf().setAppName("stacktrace-data-process-fromKafka")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  var kafkaBrokers = ""
  var outputRoot = ""
  var streaming = false

  val schema = StructType(
    StructField("stacktrace", ArrayType(StringType)) ::
      StructField("endEpoch", LongType) ::
      StructField("appId", StringType) ::
      StructField("host", StringType) ::
      StructField("name", StringType) ::
      StructField("processUuid", StringType) ::
      StructField("threadState", StringType) ::
      StructField("count", IntegerType) ::
      StructField("startEpoch", LongType) ::
      StructField("threadName", StringType) ::
      Nil
  )

  def batchProcess(prefix: String): Unit = {
    val kafkaTopic = prefix + "Stacktrace"
    val outputPath = outputRoot + "/" + prefix + "-result"

    val kStreamDF = spark.read.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic).option("startingOffsets", "earliest").load()

    kStreamDF.select(from_json(col("value").cast("string"), schema) as "js")
      .filter(size(column("js").getField("stacktrace")) > 0)
      .select(column("js").getField("stacktrace") as "stacktrace",
        column("js").getField("count") as "count")
      .select(concat(concat_ws(";", reverse('stacktrace)), lit(" "), 'count))
      .write.csv(outputPath)
  }

  def streamingProcess(prefix: String): Unit = {
    val kafkaTopic = prefix + "_Stacktrace"
    val outputPath = outputRoot + "/" + prefix + "-result"
    val checkpointLocation = outputRoot + "/" + prefix + "-checkpoint"

    val kStreamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic).option("startingOffsets", "latest").load()

    kStreamDF.select(from_json(col("value").cast("string"), schema) as "js")
      .filter(size(column("js").getField("stacktrace")) > 0)
      .select(column("js").getField("stacktrace") as "stacktrace",
        column("js").getField("count") as "count")
      .select(concat(concat_ws(";", reverse('stacktrace)), lit(" "), 'count))
      .writeStream.format("csv").option("path", outputPath)
      .option("checkpointLocation", checkpointLocation)
      .start.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val prefix = args(0)

    kafkaBrokers = spark.sparkContext.getConf.get("spark.kafka.brokers")
    outputRoot = spark.sparkContext.getConf.get("spark.kafka.outputRoot")
    streaming = spark.sparkContext.getConf.getBoolean("spark.kafka.streaming", false)

    if (streaming)
      streamingProcess(prefix)
    else
      batchProcess(prefix)

  }
}
