from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


def mapRed(line):
    tokens = line.split(",")
    if len(tokens) > 3:
        action = tokens[3]  # either blocked / viewed / clicked
        return action, 1
    else:
        return "Unknown", 1


spark = SparkSession \
    .builder \
    .appName("WindowedCount") \
    .getOrCreate()

sc = spark.sparkContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("../checkpoints")

lines = ssc.socketTextStream("localhost", 9999)
actions_kv_pairs = lines.map(lambda line: mapRed(line))

windowed_action_counts = actions_kv_pairs.reduceByKeyAndWindow(lambda a, b: (a + b), 10, 10)
windowed_action_counts.pprint()

ssc.start()
ssc.awaitTermination()
