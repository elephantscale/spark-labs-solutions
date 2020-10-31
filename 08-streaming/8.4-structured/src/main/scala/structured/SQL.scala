/*

1. Run the app
    $  $SPARK_HOME/bin/spark-submit  --master local[2]   --driver-class-path logging/  \
       --class structured.SQL     target/scala-2.12/structured-streaming_2.12-1.0.jar 
       
2. supply input
     $    cp   clickstream.json    input/1.json
     $    cp   clickstream.json    input/2.json
 */

package structured


import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession


object SQL {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Structured Streaming").
                getOrCreate()
    import spark.implicits._
      
    // Set loglevel to Error
    spark.sparkContext.setLogLevel("ERROR")

    println("### Spark UI available on port : " + spark.sparkContext.uiWebUrl.get.split(':')(2))


    val sample_data = spark.read.json("clickstream.json")
    println ("clickstream schema:")
    sample_data.printSchema

    val schema = sample_data.schema
    println("ClickStream sample schema is : " + schema)
      
    val click_stream = spark.readStream
                       .schema(schema)
                       .json("input/")
      
    // Print out incoming text
    // the 'withColumn' is for debug, so we can identify query outupt
    val query1 = click_stream
                    .withColumn ("query", lit("query1-clickstream"))
                    .writeStream
                    .outputMode("append")
                    .format("console")
                    .queryName("query1-clickstream")
                    .start()

   click_stream.createOrReplaceTempView("clickstream")
      
   val sql_str = """
                    select domain, count(*) as visits, SUM(cost) as spend 
                    from clickstream
                    group by domain
                    order by visits DESC
                    """

    val by_domain_results = spark.sql(sql_str)
      
    val query2 = by_domain_results
            .withColumn ("query", lit("query2-bydomain"))
            .writeStream
            .outputMode("complete")
            .option("truncate", false)
            .format("console")
            .queryName("query2-bydomain")
            .start()
      
    // wait forever until user terminate manually
    spark.streams.awaitAnyTermination() 
    spark.stop()
  }
}
