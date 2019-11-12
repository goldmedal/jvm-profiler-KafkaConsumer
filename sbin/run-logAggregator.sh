#!/bin/bash

SPARK_HOME=

INPUT=
OUTPUT=

$SPARK_HOME/bin/spark-submit \
    --master "local[3]" \
    --conf spark.driver.memory=4g \
    --properties-file  kafka.conf \
    --class com.wasai.data.profile.LogAggregator \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 \
    $(pwd)/target/scala-2.11/jvm-profiler-kafkaconsumer_2.11-0.1.jar \
    $INPUT $OUTPUT
