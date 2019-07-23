# import necessary packages
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys

if len(sys.argv) != 3:
    print("Need <brokers> <topics> ")
    print("e.g. localhost:9092  topic1 ")
else:
    brokers, topics = sys.argv[1], sys.argv[2]
    print("### reading from brokers : " + brokers + ",  topics : " + topics)

    topics_array = topics.split(",")
    kafka_params = {"bootstrap.servers": brokers,
                   "group.id": "app1",
                   "auto.offset.reset": "largest"
                   }

    # Initialize Spark
    spark = SparkSession \
        .builder \
        .appName("KafkaDirectStreaming") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 5)
	
    stream = KafkaUtils.createDirectStream(ssc, topics_array, kafka_params)

	# extract the values
    lines = stream.map(lambda record: record[1])
		
    x = lines.filter(lambda word: "x" in word)
    x.pprint()
    
    ssc.start()
    ssc.awaitTermination()
