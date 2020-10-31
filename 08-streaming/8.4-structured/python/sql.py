"""

1. Run the app
     $  cd <project root dir>
     $  rm -f input/* ; $SPARK_HOME/bin/spark-submit --master local[2] \
            --driver-class-path ../logging/ \
            python/sql.py
            
2. supply input
     $    cp   clickstream.json    input/1.json
     $    cp   clickstream.json    input/2.json
 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()

# Set loglevel to Error
spark.sparkContext.setLogLevel("ERROR")
print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


sample_data = spark.read.json("clickstream.json")
print ("clickstream schema:")
sample_data.printSchema()

schema = sample_data.schema
print("ClickStream sample schema is : " , schema)



click_stream = spark.readStream  \
               .schema(schema)  \
               .json("input/")


## print out all incoming data
query1 = click_stream \
    .withColumn ("query", lit("query1-clickstream")) \
    .writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .queryName("query1-clickstream") \
    .start()
# ----- end-TODO-2


# register a temp table
click_stream.createOrReplaceTempView("clickstream")


sql_str = (
'select domain, count(*) as visits, SUM(cost) as spend '
'from clickstream '
'group by domain '
'order by visits DESC '
)

# sql_str = """
# select domain, count(*) as visits, SUM(cost) as spend 
# from clickstream
# group by domain
# order by visits DESC
# """

# TODO-3 :
by_domain_results = spark.sql(sql_str)

query2 = by_domain_results \
    .withColumn ("query", lit("query2-bydomain")) \
    .writeStream \
    .outputMode("complete") \
    .option("truncate", False) \
    .format("console") \
    .queryName("query2-bydomain") \
    .start()
# ----- end-TODO-3


spark.streams.awaitAnyTermination() 
spark.stop()
