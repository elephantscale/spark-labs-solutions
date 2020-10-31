package x

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel


object WindowedCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("WindowedCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 10000, StorageLevel.MEMORY_ONLY)
    val actionsKVPairs = lines.map{
      line => {
        val tokens = line.split(",")
        if (tokens.length >= 3) {
          val action = tokens(3) // either blocked / viewed / clicked
          (action, 1)
        }
        else
          ("Unknown", 1)
      }
    }
    actionsKVPairs.print()

    /// TODO-1: Try 10 seconds for both both the values for window intervals
    // reduceByKeyAndWindow (reduce func,  window duration, sliding window)
    // window duration (last 10 seconds)
    // sliding window (10 seconds)
    val windowedActionCounts = actionsKVPairs.reduceByKeyAndWindow((a:Int, b:Int) => (a+b),
        Seconds(10), Seconds(10))

    windowedActionCounts.print()
    windowedActionCounts.saveAsTextFiles("out/actions")

    ssc.start()
    ssc.awaitTermination()
  }
}
