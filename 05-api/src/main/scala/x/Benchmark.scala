package x

import org.apache.spark.sql.SparkSession

/**
 * to invoke:
 */
object Benchmark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().
                appName("Process Files -- MYNAME").
                getOrCreate()
    val files = List ("s3n://elephantscale-public/data/twinkle/1M.data",
      "s3n://elephantscale-public/data/twinkle/10M.data",
      "s3n://elephantscale-public/data/twinkle/100M.data",
      "s3n://elephantscale-public/data/twinkle/500M.data",
      "s3n://elephantscale-public/data/twinkle/1G.data",
      "s3n://elephantscale-public/data/twinkle/2G.data")


    for(f <- files) {
      println ("### processing file : " + f)
      // count without caching
      val file = spark.read.textFile(f)
      for (i <- 1 to 5) {
        val t1 = System.nanoTime()
        val count = file.count()
        val t2 = System.nanoTime()
        println ("### %s : count (%,d) before caching took %,d ms".format(f, count, (t2-t1)/1000000 ))
      }

      // caching
      file.cache()
      //println ("### cached rdd : " + file.toDebugString)  // This no longer works in 2.1 

      for (i <- 1 to 5) {
        val t1 = System.nanoTime()
        val count = file.count()
        val t2 = System.nanoTime()
        println ("### %s : count (%,d) after caching took %,d ms".format(f, count, (t2-t1)/1000000 ))

      }
    }
  }
}
