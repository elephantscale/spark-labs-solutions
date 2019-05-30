package x
import org.specs2.mutable._

import scala.sys.process.Process

class ProcessFileTest extends Specification {
  "ProcessFiles" should {
    "submit a Spark job" in {
      val home = System.getProperty("user.home")
      val project = System.getProperty("user.dir")
      val testRun = home +
        "/spark/bin/spark-submit" +
        " --class x.ProcessFiles" +
        " " + project + "/" + "target/scala-2.11/testapp_2.11-1.0.jar" + " /data/text/twinkle/1G.data"

        println("testSparkSubmit")
      println("Run command: " + testRun)
      val line = Process(testRun).lineStream.headOption
      println(line.get)
      line.get.contains("count:  31,300,777 ")
    }
  }
}
