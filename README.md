Jvm Profiler KafkaConsumer
===

This project is a Kafka Consumer implemented through Apache Spark Streaming for those topics created
by jvm-profiler presented by [uber-common](https://github.com/uber-common/jvm-profiler).
We also write some script to process those information collected by kafka and
use [brendangregg-flamegraph](https://github.com/brendangregg/FlameGraph) tool to make the final flamegraph.

# How to Build

- Java: JDK 8+
- Scala: Scala 2.11
- Spark: 2.4.4

```shell
sbt clean package
```

# Properties

It's necessary to create a conf file and use `--propertiese-file` to append it when `spark-submit`.

- `spark.kafka.brokers`: the value of `kafka.bootstrap.servers`.
- `spark.kafka.outputRoot`: the value of result output.
- `spark.kafka.streaming`: set true to enable the streaming mode.

In this path, it will create two folders called `$prefix-result` and `$prefix-checkpoint` for the result and checkpoint.

# Parameter

- args(0): the prefix of the kafka topic.

# How to Run

In the home path of jvm-profiler kafakConsumer.

1. Run the consumer

```shell
./sbin/run-comsumer.sh $PREFIX
```

`$PREFIX` is the target prefix of the kafka topic you want to consume.
You can modified the config of spark in this script.

2. Run the result aggregator

```shell
./sbin/generate-result-flamegraph.sh $PREFIX $RESULT_DIR
```

`$PREFIX` is the name prefix of result svg which created by `flamegraph.pl`
`$RESULT_DIR` is the data which you want to process.

3. View the flamegraph

The final svg file will be created in `target` folder.
Use browser or other tools to view it.
