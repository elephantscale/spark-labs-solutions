/*
1. Simulate a source connection
     $   ncat -l -k -p 10000

2. Run the app
    $  $SPARK_HOME/bin/spark-submit  --master local[2]   --driver-class-path logging/  \
       --class structured.WordCount     target/scala-2.12/structured-streaming_2.12-1.0.jar 
 */


package structured


import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession


object WordCount {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Structured Streaming Word Count").
                getOrCreate()
    import spark.implicits._
   
    println("### Spark UI available on port : " + spark.sparkContext.uiWebUrl.get.split(':')(2))

    val lines = spark.readStream.format("socket").
                      option("host", "localhost").
                      option("port", 10000)
                      .load()

    println ("Lines schema:")
    lines.printSchema
      
    // Print out incoming text
    // the 'withColumn' is for debug, so we can identify query outupt
    val query1 = lines
                .withColumn ("query", lit("query1-lines"))
                .writeStream
                .outputMode("append")
                .format("console")
                .queryName("query1-lines")
                .start()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" +"))
                .withColumnRenamed("value", "word")
    println ("Words schema:")
    words.printSchema
      
    val query2 = words
                    .withColumn ("query", lit("query2-words"))
                    .writeStream
                    .outputMode("append")
                    .format("console")
                    .queryName("query2-words")
                    .start()
      
    // Generate running word count
    val wordCounts = words.groupBy("word").count()

     val query3 = wordCounts
                    .withColumn ("query", lit("query3-wordCounts"))
                    .writeStream
                    .outputMode("complete")
                    .format("console")
                    .queryName("query3-wordCounts")
                    .start()

     // wait forever until user terminate manually
    spark.streams.awaitAnyTermination() 
    spark.stop()

  }
}
