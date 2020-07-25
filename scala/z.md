[<< back to main index](../README.md) / [API labs](./README.md)

Lab 5.1 First Spark Job Submission
==================================

### Overview
Submit a job for Spark

### Depends On
None

### Run time
20-30 mins


---------------------
STEP 1: Edit source file
---------------------
Go to the project root directory
```
$    cd ~/dev/spark-labs/data/text/twinkle
```

**edit file : `src/main/scala/x/ProcessFiles.scala`**  
**And fix the TODO items**

```
$    vi  src/main/scala/x/ProcessFiles.scala
# or
$    nano  src/main/scala/x/ProcessFiles.scala
```


----------------------------------
STEP 2: Start Spark Server / Shell
----------------------------------
```
$  ~/apps/spark/sbin/start-all.sh
```

Starting Shell (with 4G memory)

#### == Scala
```
$  ~/apps/spark/bin/spark-shell  --master <spark master uri>  --executor-memory 4G
```

#### == Python:
```
$   ~/apps/spark/bin/pyspark   --master  spark-server-uri
#                                         ^^^^^^^^^^^^^^^^
#                                    update this to match your spark server

$   ~/apps/spark/bin/pyspark   --master  spark://localhost:7077
```



----------------
STEP 3: Load RDD
----------------
Load a big file (e.g 500M.data)

```scala
// scala
val f = sc.textFile("/data/twinkle/500M.data")
```

```python
# python
f = sc.textFile("/data/twinkle/500M.data")
```

**count the number of lines in this file**  
Hint : `count()`  

**Notice the time took**  
**Do the same count() operation a few times until the execution time 'stablizes'**  
**Can you explain the behavior of count() execution time ?**


--------------
STEP 4:  Cache
--------------
**cache the file using  `cache()` action.**
```
f.cache()
```

**Run the `count()` again. Notice the time.   Can you explain this behavior ?  :-)**

**Run count() a few more times and note the execution times.**  
**Do the timings make sense?**


------------------------------------
STEP 5:  Understanding Cache storage
------------------------------------
Go to spark shell UI @ port 4040  
**Inspect 'storage' tab**  

![caching](../images/3.4.png)

**Can you see the cached RDD?  What is the memory size?**  
**What are the implications?**

----------------------------
STEP 5:  Cache a larger file
----------------------------
**Try to cache 1G.data file and do count()**  
Is caching successful ?
If not, try starting Spark shell with more memory


----------------------------------
Step 6 : Reducing memory footprint
----------------------------------
There are various levels of memory caching.  Here are a couple:
* Raw caching (`rdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)`)
* Serialized Caching (`rdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)`)

**Try both options `f.persist(....)` .  Monitor memory consumption in storage tab**


-----------------
Group discussion
-----------------
* mechanics of caching
* implications of caching vs memory


-------------------------------------
BONUS LAB: Caching data from Amazon S3
-------------------------------------
We have some data files stored in Amazon S3.  Here are couple of path names
* s3n://elephantscale-public/data/text/twinkle/100M.data
* s3n://elephantscale-public/data/text/twinkle/500M.data
* ...

Try loading these files.  Measure performance time before and after caching.
```scala
// scala
val f = sc.textFile("s3n://....file location")
f.count() // measure time.. do it a couple of times
f.cache()
f.count() // measure time.. do it a couple of times
```
```python
// scala
f = sc.textFile("s3n://....file location")
f.count() // measure time.. do it a couple of times
f.cache()
f.count() // measure time.. do it a couple of times
```

---------------
Further Reading
---------------
* [Understanding Spark Caching by Sujee Maniyam](http://sujee.net/2015/01/22/understanding-spark-caching/)
