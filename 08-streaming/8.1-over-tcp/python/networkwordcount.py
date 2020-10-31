from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from operator import add

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")

ssc = StreamingContext(sc, 10)

## TODO-1: listen on port 10000, only cache it local memory
lines = ssc.socketTextStream("localhost", 10000)
lines.pprint()

words = lines.flatMap(lambda x: x.split(" "))
pairs = words.map(lambda w: (w, 1))
wordCounts = pairs.reduceByKey(add) 
wordCounts.pprint()


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
