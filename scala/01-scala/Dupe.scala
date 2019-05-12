import scala.io.Source

object Dupe {
    def main(args: Array[String]): Unit = {
      var lineNum = 0
      val uniq = scala.collection.mutable.Set[String] ()
      Source.fromFile("twinkle.txt").getLines().foreach ( line => {
        lineNum = lineNum +  1
        println( lineNum  + ":" + line)
        if (! uniq.add(line))
          println ("*** dupe!")
       })
    }
}