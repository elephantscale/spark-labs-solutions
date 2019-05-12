# Usage:
# spark-submit  processfiles.py  <files to process>
# spark-submit  --master spark://localhost:7077 processfiles.py  <files to process>

# Multiple files can be specified

#e.g:
#- with 4G executor memory and turning off verbose logging
#   spark-submit  --master spark://localhost:7077 --executor-memory 4g   --driver-class-path logging/    processfiles.py  /data/text/twinkle/1G.data
#

import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    sys.exit("need file(s) to load")

spark = SparkSession.builder.appName("Process Files -- MYNAME").getOrCreate()

for file in sys.argv[1:]:
    f = spark.read.text(file)

    t1 = time.perf_counter()
    count = f.count()
    t2 = time.perf_counter()

    print("### {}: count:  {} ,  time took:  {:,.2f} ms".format(file, count, (t2-t1)*1000))

    # end of for loop


line = input('### Hit Ctrl+C to terminate the program...')
spark.stop()  # close the session
