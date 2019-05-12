package x

/**
this will process JSON clickstream data

Usage:
spark-submit --class 'x.Clickstream'  target/scala-2.11/testapp_2.11-1.0.jar    <files to process>

*/


import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object Clickstream {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println ("need file(s) to load")
      System.exit(1)
    }

    val input = args(0) // can be wildcard  'dir/*.log'

    val spark = SparkSession.builder().
                appName("Clicks -- MYNAME").
                getOrCreate()


    val clickstream = spark.read.json(input)

    val count =  clickstream.count
    println("### total clickstream records " + count)

    val domainCount = clickstream.groupBy("domain").count()

    domainCount.orderBy("count").show()
  }
}
