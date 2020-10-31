"""

1. Simulating a source connection
     $   ncat -l -k -p 10000

2. Run the app
     $  $SPARK_HOME/bin/spark-submit --master local[2] \
        --driver-class-path ../logging/ \
        word_count.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lit

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("Structed Streaming ") \
    .getOrCreate()

# Set loglevel to Error
spark.sparkContext.setLogLevel("ERROR")

print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


## TODO-1 : read from socket 10000
lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 10000) \
        .load()

## Print out incoming text
## the 'withColumn' is for debug, so we can identify query outupt
query1 = lines \
        .withColumn ("query", lit("query1-lines")) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("query1") \
        .start()
# ----- end-TODO-1


# Split the lines into words
# Explode will create new rows
# so input : "hello world" will turn it into
#        hello
#        world
words = lines.select(
       explode(
           split(lines.value, " +")
       ).alias("word")
       )

 # Start running the query that prints the running counts to the console
query2 = words \
        .withColumn ("query", lit("query2-words")) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("query2") \
        .start()

# Generate running word count
wordCounts = words.groupBy("word").count()

 # Start running the query that prints the running counts to the console
query3 = wordCounts \
    .withColumn ("query", lit("query3-wordcounts")) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("query3") \
    .start()

# wait forever until streams terminate
spark.streams.awaitAnyTermination() 

spark.stop()
