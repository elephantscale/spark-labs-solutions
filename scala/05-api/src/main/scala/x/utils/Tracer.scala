package x.utils

class Tracer {
  var trace: List[String] = List()
  var counter = 0


  def add(newTrace : String) {
    counter += 1
    trace = trace :+ "%2d : %s".format(counter, newTrace)

  }

  def getTrace()  : List[String] = {
    trace
  }

  def getAsArray()  : Array[String] = {
    trace.toArray
  }

}

object TracerTest {

   def main(args: Array[String]) {
      val tracer1 = new Tracer()
      val tracer2 = new Tracer()
      tracer1.add("hello")
      tracer1.add("world")
      tracer2.add("goodbye")
      tracer2.add("world")
      println (tracer1.getTrace())
      println (tracer1.getAsArray().mkString("\n"))
      println (tracer2.getAsArray().mkString("\n"))
    }

}

