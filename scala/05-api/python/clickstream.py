# Usage:
# spark-submit  clickstream.py  <files to process>
# spark-submit  --master spark://localhost:7077 clickstream.py  <files to process>

## dealing with logs
# spark-submit  clickstream.py  <files to process>  2> logs
#

# Multiple files can be specified
#e.g:
#- with 4G executor memory and turning off verbose logging
#   spark-submit  --master spark://localhost:7077 --executor-memory 4g   --driver-class-path logging/    clickstream.py  /data/cickstream/json/
#

import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    sys.exit("need clickstream json to load")

spark = SparkSession.builder.appName("Process Clickstream -- MYNAME").getOrCreate()

f = sys.argv[1]
clickstream = spark.read.json(f)
count = clickstream.count()
print ("record count ", count)

domainCount = clickstream.groupBy("domain").count()
domainCount.orderBy("count", ascending=False).show()

#line = input('### Hit Ctrl+C to terminate the program...')
spark.stop()  # close the session
