// uses old style Receivers

package x

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming._

/*
  $  sbt assembly

Example:
  $   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  --class x.KafkaStreaming  target/scala-2.10/kafka-assembly-1.0.jar   localhost:2181   group1   clickstream 1


generic usage:
$   spark-submit  --master local[2]   --driver-class-path logging/  --class x.KafkaStreaming  target/scala-2.10/kafka_2.10-1.0.jar
 zoo01,zoo02,zoo03    my-group   topic1,topic2    1

 */
object KafkaStreaming {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: StreamingMain <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("Kafka Streaming")
    val ssc =  new StreamingContext(sparkConf, Seconds(5))
    //ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    stream.print()


    // TODO-1 : extract the data from stream tuple 
    // stream tuple (???, value);   
    // use tuple notation to get 2nd element out
    //val lines = stream.map(???)

    // TODO-2   : print the lines

    // TODO-3 : extract 'clicked' lines
    // val clicks = lines.???

    // TODO-4 : print clicks lines

    ssc.start()
    ssc.awaitTermination()
  }

}
