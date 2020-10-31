/*

1. Simulate a source connection
     $   ncat -l -k -p 10000

2. Run the app
    $  $SPARK_HOME/bin/spark-submit  --master local[2]   --driver-class-path logging/  \
       --class structured.Intro     target/scala-2.12/structured-streaming_2.12-1.0.jar 
 */

package structured


import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession


object Intro {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Structured Streaming").
                getOrCreate()
    import spark.implicits._
      
    // Set loglevel to Error
    spark.sparkContext.setLogLevel("ERROR")

    println("### Spark UI available on port : " + spark.sparkContext.uiWebUrl.get.split(':')(2))

    val lines = spark.readStream.format("socket").
                      option("host", "localhost").
                      option("port", 10000)
                      .load()

    lines.printSchema
      
    // Print out incoming text
    // the 'withColumn' is for debug, so we can identify query outupt
    val query1 = lines
                    .withColumn ("query", lit("query1"))
                    .writeStream
                    .outputMode("append")
                    .format("console")
                    .queryName("query1")
                    .start()

    val x = lines.filter(lines("value").contains("x"))
      
      
    val query2 = x
                    .withColumn ("query", lit("query2"))
                    .writeStream
                    .outputMode("append")
                    .format("console")
                    .queryName("query2")
                    .start()

    // wait forever until user terminate manually
    spark.streams.awaitAnyTermination() 
    spark.stop()
  }
}
