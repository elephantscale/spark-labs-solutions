import MyTimer._

object TestTimer {
    
    def sleepy (duration : Int) = {
        Thread.sleep(duration)
        println ("yawn... slept for " + duration)
        duration
    }

    def main(args: Array[String]): Unit = {
      val a = MyTimer.timeit(sleepy(300))
      println ("result 1 : " + a)

      // simple expressions
      timeit ({ 1 + 2})
      // timeit {1 + 2}  // no brackets needed

      // calculating square of million numbers
      val r = 1 to 1000000
      timeit { r. map (x => x*x)}
    }
}