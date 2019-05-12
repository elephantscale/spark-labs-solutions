package x

import org.apache.spark.sql.SparkSession


/*
Usage:
spark-submit --class 'x.ProcessFiles' --master spark://localhost:7077  target/scala-2.11/testapp_2.11-1.0.jar    <files to process>

Multiple files can be specified
file to process can be :  /etc/hosts
                          scripts/1M.data
                          s3n://elephantscale-public/data/text/twinkle/100M.data
                          tachyon://tachyon_ip_address:19998/file

e.g:
- with 4G executor memory and turning off verbose logging
    spark-submit --class x.ProcessFiles  --master spark://localhost:7077 --executor-memory 4g   --driver-class-path logging/   target/scala-2.11/testapp_2.11-1.0.jar   s3n:///elephantscale-public/data/text/twinkle/1G.data

*/
object ProcessFiles {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("need file(s) to load")
      System.exit(1)
    }

    val spark = SparkSession.builder().
                appName("Process Files -- solution").
                getOrCreate()

    var file = ""
    for (file <- args) {
      val f = spark.read.textFile(file)
      val t1 = System.nanoTime()
      val count = f.count()
      val t2 = System.nanoTime()

      println("### %s: count: %,d , time took: %,f ms".format(file, count, (t2 - t1) / 1e6))
    }
    // ## TIP :  uncomment below to keep App UI (port 4040) alive :-)
    // println("Hit enter to terminate the program...:")
    // val line = Console.readLine
    
    // ## TO terminate script automatically 
    // ## (without waiting for console input) use '/dev/null' as follows
    // ##   $   spark-submit .....usual args.........    < /dev/null


    spark.stop()  // close the session
  }
}
