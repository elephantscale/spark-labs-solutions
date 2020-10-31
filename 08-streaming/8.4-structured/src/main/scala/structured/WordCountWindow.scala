/*
1. Simulate a source connection
     $   ncat -l -k -p 10000

2. Run the app
    $  $SPARK_HOME/bin/spark-submit  --master local[2]   --driver-class-path logging/  \
       --class structured.WordCountWindow     target/scala-2.12/structured-streaming_2.12-1.0.jar 
 */


package structured


import org.apache.spark.sql.functions.{current_timestamp, window, lit}
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp


object WordCountWindow {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Structured Streaming Word Count").
                getOrCreate()
    import spark.implicits._
    println("### Spark UI available on port : " + spark.sparkContext.uiWebUrl.get.split(':')(2))

    // we can include timestamp like this also 
    //          .option('includeTimestamp', 'true')\
    val lines = spark.readStream.
                      format("socket").
                      option("host", "localhost").
                      option("port", 10000).
                      load().
                      withColumn ("timestamp", current_timestamp)
                   
    println ("Lines schema:")
    lines.printSchema
      
    // Print out incoming text
    // the 'withColumn' is for debug, so we can identify query outupt
    val query1 = lines
                .withColumn ("query", lit("query1-lines"))
                .writeStream
                .outputMode("append")
                .option("truncate", false)
                .format("console")
                .queryName("query1-lines")
                .start()
    
    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
                  line._1.split(" +").map(word => (word, line._2))
                ).toDF("word", "timestamp")
    println ("words schema:")
    words.printSchema
      
    val query2 = words
                    .withColumn ("query", lit("query2-words"))
                    .writeStream
                    .outputMode("append")
                    .option("truncate", false)
                    .format("console")
                    .queryName("query2-words")
                    .start()
    
      
    // Window function is : window( column, window_interval,  slide_interval)  
    val windowedCounts = words.groupBy (
        window($"timestamp", "10 seconds"),  // window (column, window_interval)
        // window($"timestamp", "10 seconds", "5 seconds"),  // window (column, window_interval, slide_interval)
        $"word")
      .count()
      .orderBy("window")
      
    val query3 = windowedCounts
                    .withColumn ("query", lit("query3-windowedCounts"))
                    .writeStream
                    .outputMode("complete")
                    .option("truncate", false)
                    .format("console")
                    .queryName("query3-windowedCounts")
                    .start()

        // wait forever until user terminate manually
    spark.streams.awaitAnyTermination() 
    spark.stop()
  }
}
