package x


import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

/*
$   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  --class x.NetworkWordCount target/scala-2.11/over-tcp_2.11-1.0.jar
 */


object NetworkWordCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // listen on port 9999, only cache it local memory
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    lines.print

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(w => (w, 1))
    val wordCounts = pairs.reduceByKey(_ + _) 
    wordCounts.print()


    // kick off the processing
    ssc.start()
    ssc.awaitTermination()
  }
}

