#!/bin/bash

PREFIX=$1
TARGET=target

if [ -z "$PREFIX" ]
then
    echo "[ERROR] PREFIX is emptyp."
    echo "[ERROR] You need to assign a prefix to get kafka topic"
else
    spark-submit \
        --master "local[3]" \
        --conf spark.driver.memory=4g \
        --properties-file  kafka.conf \
        --class jax.data.profile.KafkaResultConsumer \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 \
        $(pwd)/target/scala-2.11/jvm-profiler-kafkaconsumer_2.11-0.1.jar \
        $PREFIX
fi
