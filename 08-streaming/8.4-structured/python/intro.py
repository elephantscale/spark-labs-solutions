"""
Simulating a source connection
     $   ncat -l -k -p 10000

Run the app
     $   $SPARK_HOME/bin/spark-submit --master local[2] \
         --driver-class-path ../logging/ \
         intro.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("Structed Streaming") \
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
# ----- end-TODO-1

## Print out incoming text
## the 'withColumn' is for debug, so we can identify query outupt
query1 = lines \
    .withColumn ("query", lit("query1")) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("query1") \
    .start()


## TODO-2  :filter lines that has 'x'
x = lines.filter(lines["value"].contains("x"))
# ----- end-TODO-2


## the 'withColumn' is for debug, so we can identify query outupt
## TODO-3 : To run query2 , comment out query1
query2 = x \
        .withColumn ("query", lit("query2")) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("query2") \
        .start()
# ----- end-TODO-3


# wait forever until streams terminate
spark.streams.awaitAnyTermination() 

spark.stop()
