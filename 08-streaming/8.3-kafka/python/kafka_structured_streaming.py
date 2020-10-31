# import necessary packages
from pyspark.sql import SparkSession
import sys
from datetime import time

topic = "test"

if len(sys.argv) == 2:
    topic = sys.argv[1]

spark = SparkSession \
    .builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# Option 1 without
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", topic) \
#     .option("startingOffsets", "latest").load()
#

# option 2: specify schema
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic).option("startingOffsets", "latest") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)",
                       "CAST(partition AS INTEGER)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

df.printSchema()


query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .queryName("Read from Kafka topic:" + topic) \
    .start()

# simple, wait for ever
query.awaitTermination()

spark.stop()
