"""

1. Simulating a source connection
     $   ncat -l -k -p 10000

2. Run the app
     $  $SPARK_HOME/bin/spark-submit --master local[2] \
            --driver-class-path ../logging/ \
            word_count_window.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, split, current_timestamp, window


# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("Structed Streaming ") \
    .getOrCreate()

# Set loglevel to Error
spark.sparkContext.setLogLevel("ERROR")

print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10000) \
    .option('includeTimestamp', 'true')\
    .load() 

## Or we can include timestamp like this too
# lines = spark \
#     .readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 10000) \
#     .load() \
#     .withColumn ("timestamp", current_timestamp())

lines.printSchema()

## print out all incoming data
query1 = lines \
    .withColumn ("query", lit("query1-lines")) \
    .writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .queryName("query1-lines") \
    .start()


# Split the lines into words, retaining timestamps
# split() splits each line into an array, and explode() turns the array into multiple rows
words = lines.select(
    explode(split(lines.value, " +")).alias('word'),
    lines.timestamp
)

words.printSchema()


## print out words
query2 = words \
    .withColumn ("query", lit("query2-words")) \
    .writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .queryName("query2-words") \
    .start()

# # Window function is : window( column, window_interval,  slide_interval)  
windowdCounts = words.groupBy (
    window(words.timestamp, "10 seconds"),  # window (column, window_interval)
    # window(words.timestamp, "10 seconds", "5 seconds"),  # window (column, window_interval, slide_interval)
    words.word) \
    .count() \
    .orderBy("window") # optional



# # Start running the query that prints the running counts to the console
query3 = windowdCounts \
    .withColumn ("query", lit("query3-windowCounts")) \
    .writeStream \
    .outputMode("complete") \
    .option("truncate", False) \
    .format("console") \
    .queryName("query3-windowCounts") \
    .start()


# wait forever until user terminate manually
spark.streams.awaitAnyTermination()

spark.stop()
