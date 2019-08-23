package x


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/*
$  spark-submit  --master local[2]   --driver-class-path logging/  --class x.JSONStructuredStreaming  target/scala-2.11/structured-streaming_2.11-1.0.jar
 */


object JSONStreaming {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Structured Streaming").
                getOrCreate()

    // TODO-1 figure out the schema, uncomment the following block
    
    // figure out clickstream schema using sample file
    
    val sample = spark.read.json("file:///data/click-stream/clickstream.json")
    sample.printSchema
    val schema = sample.schema
    println ("Clickstream sample schema is : " + schema)
    
    // ----- end-TODO-1

    
    // Simple Streaming
    // TODO-2 : uncomment the following block to get streaming going
    
    val inputPath = "json-input"
    println ("Reading from : " + inputPath)
    val clickstream = spark.readStream.format("json").
                      schema(schema).
                      json(inputPath)

    val echo = clickstream.writeStream.
                outputMode("append")
                .format("console")
                .start()
    
    //  -------  end TODO-2
    
    
    // TODO-3 - query1 : aggregate query
    
    val byDomain = clickstream.groupBy("domain").count
    val query1 = byDomain.writeStream
                  .outputMode("complete")
                  .format("console")
                  .start()
    
    // ----- end-TODO-3
    

    // TODO-4 query2 : filter
    
    val blocked = clickstream.filter("action == 'blocked'")
    val query2 = blocked.writeStream.
                outputMode("append").
                format("console")
                .start()
    
    // ----- end TODO-4
    

    // run application for ever
    spark.streams.awaitAnyTermination()  
  }


  
}

