package x


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/*
$  spark-submit  --master local[2]   --driver-class-path logging/  --class x.JSONStreaming  target/scala-2.11/structured-streaming_2.11-1.0.jar
 */


object JSONStreaming {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Structured Streaming").
                getOrCreate()

    // figure out clickstream schema using sample file
    val sample = spark.read.json("clickstream.json")
    sample.printSchema
    val schema = sample.schema
    println ("Clickstream sample schema is : " + schema)


    // Simple Streaming
    val inputPath = "json-input"
    println ("Reading from : " + inputPath)
    val clickstream = spark.readStream.format("json").
                      schema(schema).
                      json(inputPath)

    val echo = clickstream.writeStream.
                outputMode("append")
                .format("console")
                .start()

    // aggregate query
    val byDomain = clickstream.groupBy("domain").count
    val query1 = byDomain.writeStream
                  .outputMode("complete") // complete, append, update
                  .format("console")
                  .start()

    //filter query
    val blocked = clickstream.filter("action == 'blocked'")
    val query2 = blocked.writeStream.
                outputMode("append").
                format("console")
                .start()

    spark.streams.awaitAnyTermination()  
  }


}

