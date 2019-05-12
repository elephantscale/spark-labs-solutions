import org.apache.spark._
import org.apache.spark.streaming._


object JoinD {
def main(args: Array[String]) {


val ssc = new StreamingContext("local[2]", "JoinD", Seconds(2)) 


val lines = ssc.socketTextStream("127.0.0.1", 9999)

//TODO : Try this lab by commeting the 1st line (blocked). If you dont get any data, then try by uncommeting this line and make relevant mods to rest of the lines

val linesWBlocked = lines.filter(line=>line.contains("blocked"))
val linesWClicked = lines.filter(line=>line.contains("clicked"))
val linesWViewed = lines.filter(line=>line.contains("viewed"))

val blocked = linesWBlocked.flatMap(_.split(","))
val blockedpairs = blocked.map(word => (word, 1))


val clicked = linesWClicked.flatMap(_.split(","))
val clickedpairs = clicked.map(word => (word, 1))

val viewed = linesWViewed.flatMap(_.split(","))
val viewedpairs = viewed.map(word => (word, 1))


//1st argument is Windows interval and 2nd argument is Sliding Interval

val blockedCounts = blockedpairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(2), Seconds(2))
val clickedCounts = clickedpairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(4), Seconds(2))
val viewedCounts = viewedpairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(4), Seconds(2))

//TODO : Try joining without changing any of the slide durations and see what happens. 
//TODO : Then make the slide durations same

//TODO: Comment and uncomment relevant lines based on what data you are going to use and what data you are expecting. 

val cbKeys = clickedCounts.join(blockedCounts)
val vcKeys = viewedCounts.join(clickedCounts)


blockedCounts.print()
clickedCounts.print()
viewedCounts.print()

// Comment above 3 print lines and now try to save it into a file and then into a specific folder. 
// If you want to save them in files 
//TODO: Try saving the files in a log folder by changing "Blk.txt" to "log/Blk.txt" etc. Uncomment. 


cbKeys.saveAsTextFiles("log/cb.txt")
vcKeys.saveAsTextFiles("log/vc.txt")

//clickedCounts.saveAsTextFiles("Clk.txt")
//viewedCounts.saveAsTextFiles("Vid.txt")

ssc.start()
ssc.awaitTermination()
} 
}
