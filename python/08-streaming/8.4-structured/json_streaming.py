# import necessary packages
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()
	
# Set loglevel to Error
spark.sparkContext.setLogLevel("ERROR")


# TODO-1 :	
sample_data = spark.read.json("file:///data/click-stream/clickstream.json")
sample_data.printSchema()

schema = sample_data.schema
print("ClickStream sample schema is : " , schema)	
# ----- end-TODO-1



# TODO-2 :
click_stream = spark.readStream.schema(schema).option("inferSchema", "true").json("../json-input/")
query = click_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# ----- end-TODO-2



# TODO-3 :
by_domain = click_stream.groupBy("domain").count()
query1 = by_domain.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
# ----- end-TODO-3



# TODO-4 :
blocked = click_stream.filter("action == 'blocked'")
query2 = blocked.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# ----- end-TODO-4


spark.streams.awaitAnyTermination() 
spark.stop()
