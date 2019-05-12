// uses new style Direct Streaming

package x

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/*
  $  sbt assembly

  $   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  --class x.KafkaDirectStreaming    target/scala-2.11/kafka-streaming-assembly-1.0.jar  localhost:9092  clickstream

  $   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  --class x.KafkaDirectStreaming    target/scala-2.11/kafka-streaming-assembly-1.0.jar  host1:9092,host2:9092  topic1,topic2

 */


object KafkaDirectStreaming {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Need <brokers> <topics> ")
      System.err.println("e.g. localhost:9092  topic1 ")
      System.exit(1)
    }

    val Array(brokers, topics) = args
    println ("### reading from brokers : " + brokers + ",  topics : " + topics)
    val sparkConf = new SparkConf().setAppName("Kafka Streaming")
    val ssc =  new StreamingContext(sparkConf, Seconds(5))
    //ssc.checkpoint("checkpoint")
    val topicsArray = topics.split(",")
    val kafkaParams = Map[String, Object] (
                        "bootstrap.servers" -> brokers,
                        "key.deserializer" -> classOf[StringDeserializer],
                        "value.deserializer" -> classOf[StringDeserializer],
                        "group.id" -> "app1",
                        "auto.offset.reset" -> "latest"
                      )

    val stream = KafkaUtils.createDirectStream[String,String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](topicsArray, kafkaParams)
            )

    // debug print
    // stream.map(record=>(record.value().toString)).print

    // extract the values
    val lines = stream.map(record => record.value.toString)
    lines.print

    /*
    // TODO-1 : extract lines that has 'x'
    val x = lines.filter(_.contains("???"))
    x.print
    */

    ssc.start()
    ssc.awaitTermination()
  }

}
