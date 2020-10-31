package x


import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types._
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


/*
Testing:

1 - launch spark program
$  sbt package
$  ~/spark/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0\
      --master local[2]   --driver-class-path logging/  \
      --class x.KafkaStructuredStreaming  target/scala-2.11/kafka-streaming-assembly-1.0.jar   "topic_name"

2 - Produce data
  $   ~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092\
        --property "parse.key=true" --property "key.separator=:" --topic test
          <type something here>

*/


object KafkaStructuredStreaming {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    var topic = "test"
    if (args.length == 1)
      topic = args(0)

    logger.info("### Reading from topic: " + topic)


    val spark = SparkSession.builder.appName("Read from Kafka").
      getOrCreate()

    // option 1 : default schema
    val df = spark.readStream.
                    format("kafka").
                    option("kafka.bootstrap.servers", "localhost:9092").
                    option("subscribe", topic).
                    option("startingOffsets", "latest").
                    load()

    /*
    // option 2 : specify schema
    import spark.implicits._
    val df = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", topic).
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "CAST(topic AS STRING)",
        "CAST(partition AS INTEGER)",
        "CAST(offset AS LONG)",
        "CAST(timestamp AS TIMESTAMP)").
      as[(String, String, String, Integer, Long, Timestamp)]
      */


    // choose attributes and schema
    /*
    selectExpr("CAST(value AS STRING)",
    "CAST(topic as STRING)",
    "CAST(partition as INTEGER)").
    as[(String, String, Integer)]

    selectExpr("CAST(key AS STRING)",
                    "CAST(value AS STRING)",
                    "CAST(topic AS STRING)",
                    "CAST(partition AS INTEGER)",
                    "CAST(offset AS LONG)",
                    "CAST(timestamp AS TIMESTAMP)").
                    as[(String, String, String, Integer, Long, Timestamp)]
    */


    df.printSchema

    val query = df.writeStream.
      outputMode("append").
      format("console").
      queryName("Read from Kafka topic:" + topic).
      start()

    // more options
    /*
    val query = df.writeStream.
      outputMode("append").
      format("console").
      option("truncate", true).
      trigger(Trigger.ProcessingTime(5.seconds)).
      outputMode(OutputMode.Update).
      start()
      */


    // simple, wait for ever
    query.awaitTermination()

    /*
    // more advanced, print status frequently
    while (true) {
      logger.info("query status: " + query.status)
      logger.info("query progress: " + query.lastProgress)
      query.awaitTermination(10000) // 10 secs
    }
    */

    spark.stop

  }
}
