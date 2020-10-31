import scala.io.Source
import java.io.File
import java.io.PrintWriter

object Dupe {
	def main(args: Array[String]): Unit = {
		
		// declare an *mutable* Set.  See slides for examples
		val s = scala.collection.mutable.Set[String] ()
		var lineNumber = 0
		var duplicateLines = 0
		
		// For Bonus Lab 2
		val writer = new PrintWriter(new File("dupes.txt"))
		
		Source.fromFile("twinkle.txt").getLines().foreach ( line => {
			//println(line)
			
			lineNumber = lineNumber +  1
			print( lineNumber  + ":" + line)
			
			// try adding to set
			val added = s.add(line)
			if (added) {
				println()                
			}
			else{
				println(" *** dupe!")
				duplicateLines = duplicateLines + 1
				writer.write(line + "\n")
			}			
		})
		
		// Bonus Lab 1
		println("total lines :  " + lineNumber +  ",   dupe lines : " + duplicateLines)	
		
		writer.close()
	}
}
