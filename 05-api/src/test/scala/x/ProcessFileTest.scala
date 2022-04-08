package x
import org.specs2.mutable._

import scala.sys.process.Process

class ProcessFileTest extends Specification {
  "ProcessFiles" should {
    "submit a Spark job" in {
      //val project = System.getProperty("user.dir")
      val testRun = "/home/mark/apps/spark/bin/spark-submit" +
        " --master spark://mark-workstation:7077" +
        " --class x.ProcessFiles" +
        " target/scala-2.12/testapp_2.12-1.0.jar" + " /data/text/twinkle/*.data"

      println("Shalom, testSparkSubmit")
      println("Run command: " + testRun)
      val line = Process(testRun).lineStream.headOption
      println("Output line: " + line.get)
      line.get.contains("count:  123,794,576 ")
    }
  }
}
