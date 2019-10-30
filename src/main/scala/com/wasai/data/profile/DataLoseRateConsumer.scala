package com.wasai.data.profile

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataLoseRateConsumer {

  val conf = new SparkConf().setAppName("stacktrace-data-process-fromKafka")
  val spark = SparkSession.builder().config(conf).getOrCreate()

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

  def batchProcess(prefix: String): (Long, Long) = {
    val kafkaTopic = prefix + "_Stacktrace"

    val kStreamDF = spark.read.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic).option("startingOffsets", "earliest").load()

    val profileInfo = kStreamDF.select(from_json(col("value").cast("string"), schema) as "js")
    val totalNum = profileInfo.count()
    val loseNum =   profileInfo.filter(size(column("js").getField("stacktrace")) < 0).count()
    (totalNum, loseNum)
  }

  def main(args: Array[String]): Unit = {

    val prefix = args(0)

    kafkaBrokers = spark.sparkContext.getConf.get("spark.kafka.brokers")
    val (totalNum, loseNum) = batchProcess(prefix)

    println("--------- Stacktrace Losing Information --------------")
    println("")
    println(s"the prefix of the topic: $prefix")
    println(s"Total: $totalNum, Lose: $loseNum, Losing Rate: ${loseNum / totalNum}")

  }
}
