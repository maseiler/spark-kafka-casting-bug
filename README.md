# Spark Kafka Casting Bug

Spark seems to be unable to cast the key (or value) part from Kafka to a `long` value and throws
```
org.apache.spark.sql.AnalysisException: cannot resolve 'CAST(`key` AS BIGINT)' due to data type mismatch: cannot cast binary to bigint;;
```
 This repo demonstrates the problem.

`KafkaProdcuerLongValue` produces messages to Kafka of type `<long, byte[]>` to the topic `spark-long-test`. `ReadLongFromKafka` consumes messages from this topic and prints the processed content to console. This only works, if the key (which is a `long` in Kafka) remains a byte array or is casted to a `string`. 

*Note: the same issue also occurs with `int` instead of `long`*
## Running the Demo
*Note: the code examples assumes that you are at your Kafka installation folder*
1. run Kafka instance
    - `bin/zookeeper-server-start.sh config/zookeeper.properties`
    - `bin/kafka-server-start.sh config/server.properties`
2. create topic
    - `bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic spark-long-test --partitions 1 --replication-factor 1`
3. run console consumer to verify incoming data
    - ```
      bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
             --topic spark-long-test \
             --formatter kafka.tools.DefaultMessageFormatter \
             --property print.key=true \
             --property print.value=true \
             --property key.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
             --property value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer \
             --property print.timestamp=true
      ```
4. run `ReadLongFromKafka`
5. run `KafkaProdcuerLongValue`

