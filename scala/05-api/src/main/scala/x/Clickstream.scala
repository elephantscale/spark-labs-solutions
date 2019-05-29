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
    import spark.implicits._

    println ("### Ahoy mate, fix TODO items!")

    // ## TODO-1 : create an input df;
    // hint : spark.read.json(input)
    /*
    val clickstream = ???

    // TODO-2  : count how many records we have
    val count =  ???
    println("### total clickstream records " + count)
    */

    // TODO-3 : count traffic per domain
    /*
    val domainCount = clickstream.groupBy("???").count()

    // display domainCount (hint : show)
    domainCount.???
    */

  }
}
