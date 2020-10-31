from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "OverTCP")

## TODO-1 : Create a streaming context with 5 second
## batch interval
## Hint : 5
ssc = StreamingContext(sc, 5)

## TODO-2 Create a DStream on localhost:10000
lines = ssc.socketTextStream("localhost", 10000)
lines.pprint()


## TODO-3 : filter for lines that has 'blocked'
blocked= lines.filter(lambda line : "blocked" in line)
blocked.pprint()

## TODO-4 : Save both RDDs 
blocked.saveAsTextFiles("out-blocked")


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
