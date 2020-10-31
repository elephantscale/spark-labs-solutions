import scala.io.Source

object DupeApp extends App {
    Source.fromFile("twinkle.txt").getLines().foreach ( line => {
    println(line)
    })
}
