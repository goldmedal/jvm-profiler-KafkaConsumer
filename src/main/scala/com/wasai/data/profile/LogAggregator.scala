package com.wasai.data.profile

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object LogAggregator {

  val conf = new SparkConf().setAppName("log-aggregate").setMaster("local[3]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val input = args(0)
    val output = args(1)

    val schema = StructType(StructField("callstacks", StringType) ::
      StructField("count", IntegerType) :: Nil)

    val pattern = ".*org\\.apache\\.spark\\.executor\\.Executor\\$TaskRunner\\.run.*"

    val df = spark.read.format("csv").option("sep", " ").schema(schema).load(input)
    df.groupBy('callstacks).agg(sum('count))
       .filter('callstacks rlike pattern)
      .repartition(10)
      .write.format("csv").option("sep", " ").save(output)

  }
}
