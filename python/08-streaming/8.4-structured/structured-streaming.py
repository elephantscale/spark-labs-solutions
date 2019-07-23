# import necessary packages
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("Structed Streaming") \
    .getOrCreate()

# Set loglevel to Error
spark.sparkContext.setLogLevel("ERROR")


## TODO-1 : read from socket 10000
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10000) \
    .load()
# ----- end-TODO-1


# query1 = lines \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


## TODO-2  :filter lines that has 'x'
x = lines.filter(lines["value"].contains("x"))
# ----- end-TODO-2



## TODO-3 : To run query2 , comment out query1
query2 = x \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
# ----- end-TODO-3


# wait forever until user terminate manually
spark.streams.awaitAnyTermination() 

spark.stop()
