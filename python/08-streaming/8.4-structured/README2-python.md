<title>Spark labs : 8.1 Streaming Over TCP</title></head>
<link rel='stylesheet' href='../../assets/css/main.css'/>

[<< back to main index](../../README.md)

Lab 8.4 - Structured Streaming 2 (JSON)
==================================

### Overview
Run a Spark Structured Streaming job by analyzing JSON data

### Depends On
None

### Run time
30-40 mins


## STEP 1: Go to project directory
```bash
    $ cd ~/dev/spark-labs/08-streaming/8.4-structured/python
```

## Step 2 : Inspect file
**Inspect file : `json_streaming.py`**  

## Step 3: Complete TODO-1
Uncomment code block around TODO-1 (and only this one), so your code looks like this.  
Delete """  and """ around the TODO blocks

```bash
# TODO-1 :	
sample_data = spark.read.json("file:///data/click-stream/clickstream.json")
sample_data.printSchema()

schema = sample_data.schema
print("ClickStream sample schema is : " , schema)
# ----- end-TODO-1
```

Save the file.

## Step 4: Run the streaming application

```bash
    # be in project root directory
    $ cd ~/dev/spark-labs/08-streaming/8.4-structured

    $ ~/dev/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  json_streaming.py
```

```console
root
 |-- action: string (nullable = true)
 |-- campaign: string (nullable = true)
 |-- cost: long (nullable = true)
 |-- domain: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- session: string (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- user: string (nullable = true)

Clickstream sample schema is : StructType(StructField(action,StringType,true), StructField(campaign,StringType,true), StructField(cost,LongType,true), StructField(domain,StringType,true), StructField(ip,StringType,true), StructField(session,StringType,true), StructField(timestamp,LongType,true), StructField(user,StringType,true))
```

## Step 6:  Fix TODO-2
Edit file : `json_streaming.py`**  
Uncomment code block around TODO-2

```bash
    # be in project root directory
    $ cd ~/dev/spark-labs/08-streaming/8.4-structured
    $ rm -f json-input/*  ;  
        ~/dev/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  json_streaming.py
```
Note : `rm -f json-input/*`  is used to clear the input directory

Leave this terminal running (we will call it Spark terminal)

Open another terminal and issue the following commands.

```bash
    $ cd ~/dev/spark-labs/08-streaming/8.4-structured/

    $ ln /data/click-stream/clickstream.json json-input/1.json
```

In Spark terminal you should see the first batch output

```console
-------------------------------------------
Batch: 0
-------------------------------------------
+-------+-----------+----+-----------------+----+----------+-------------+------+
| action|   campaign|cost|           domain|  ip|   session|    timestamp|  user|
+-------+-----------+----+-----------------+----+----------+-------------+------+
|clicked|campaign_19| 118|      youtube.com|ip_4|session_36|1420070400000|user_9|
|blocked|campaign_12|   5|     facebook.com|ip_3|session_96|1420070400864|user_5|
|clicked| campaign_3|  54|sf.craigslist.org|ip_9|session_61|1420070401728|user_8|
|blocked|campaign_18| 110|    wikipedia.org|ip_5|session_55|1420070402592|user_6|
|clicked| campaign_6|  15|comedycentral.com|ip_9|session_49|1420070403456|user_4|
|blocked| campaign_9| 139|          cnn.com|ip_8|session_13|1420070404320|user_7|
....

**=>  Hit Ctrl+C  on terminal #1 to kill Spark streaming application**

## Step 7 : TODO-3 (Query1)
Edit file : `json_streaming.py`**  
Uncomment code block around TODO-3

Run streaming application

```bash
    # be in project root directory
    $   cd ~/dev/spark-labs/08-streaming/8.4-structured/python
    $   rm -f json-input/*  ;  
        ~/dev/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  json_streaming.py
```

Copy files into `json-input` directory as follows.

```bash
    $   cd ~/dev/spark-labs/08-streaming/8.4-structured
    $   ln /data/click-stream/clickstream.json json-input/1.json
```


Spark will calculate results for query1:

```console
-------------------------------------------
Batch: 0
-------------------------------------------
// echo output
+-------+-----------+----+-----------------+----+----------+-------------+------+
| action|   campaign|cost|           domain|  ip|   session|    timestamp|  user|
| viewed| campaign_1|  24|        yahoo.com|ip_4|session_60|1420070411232|user_8|
|....
|blocked|campaign_19|  23|       amazon.com|ip_5|session_85|1420070415552|user_7|
| viewed|campaign_20| 133|       google.com|ip_9|session_69|1420070416416|user_7|
+-------+-----------+----+-----------------+----+----------+-------------+------+

// query1 output
+-----------------+-----+
|           domain|count|
+-----------------+-----+
|      nytimes.com|    1|
|      youtube.com|    2|
|        zynga.com|    1|
|       google.com|    2|
|        yahoo.com|    1|
|     facebook.com|    1|
|          cnn.com|    1|
|    wikipedia.org|    3|
|       sfgate.com|    1|
|       amazon.com|    2|
|   funnyordie.com|    1|
|sf.craigslist.org|    2|
|comedycentral.com|    2|
+-----------------+-----+
```

Copy more files and see the `domain count` change

```bash

    $ ln /data/click-stream/clickstream.json  json-input/2.json
    $ ln /data/click-stream/clickstream.json  json-input/3.json
```

**=>  Hit Ctrl+C  on terminal #1 to kill Spark streaming application**

## Step 8 : TODO-4  (Query2)
Edit file : `json_streaming.py`**  
Uncomment code block around TODO-4

Run streaming application

```bash
    # be in project root directory
    $ cd ~/dev/spark-labs/08-streaming/8.4-structured
    $ rm -f json-input/*  ;  
        ~/dev/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  json_streaming.py
```

Copy files into `json-input` directory as follows.

```bash
    $   cd ~/dev/spark-labs/08-streaming/8.4-structured
    $   ln /data/click-stream/clickstream.json    json-input/1.json
    $   ln /data/click-stream/clickstream.json    json-input/2.json
    $   ln /data/click-stream/clickstream.json    json-input/3.json
```

```console
-------------------------------------------
Batch: 0
-------------------------------------------
+-------+-----------+----+-----------------+----+----------+-------------+------+
| action|   campaign|cost|           domain|  ip|   session|    timestamp|  user|
+-------+-----------+----+-----------------+----+----------+-------------+------+
|clicked|campaign_19| 118|      youtube.com|ip_4|session_36|1420070400000|user_9|
|blocked|campaign_12|   5|     facebook.com|ip_3|session_96|1420070400864|user_5|
....
| viewed|campaign_20| 133|       google.com|ip_9|session_69|1420070416416|user_7|
+-------+-----------+----+-----------------+----+----------+-------------+------+

-------------------------------------------
Batch: 0
-------------------------------------------
+-------+-----------+----+-----------------+----+----------+-------------+------+
| action|   campaign|cost|           domain|  ip|   session|    timestamp|  user|
+-------+-----------+----+-----------------+----+----------+-------------+------+
|blocked|campaign_12|   5|     facebook.com|ip_3|session_96|1420070400864|user_5|
|blocked|campaign_18| 110|    wikipedia.org|ip_5|session_55|1420070402592|user_6|
|blocked| campaign_9| 139|          cnn.com|ip_8|session_13|1420070404320|user_7|
|blocked| campaign_4| 171|   funnyordie.com|ip_1|session_92|1420070405184|user_9|
|blocked|campaign_17|  20|       amazon.com|ip_4|session_13|1420070406048|user_1|
|blocked|campaign_20|  78|        zynga.com|ip_5|session_36|1420070406912|user_3|
|blocked|campaign_19| 147|      nytimes.com|ip_2|session_65|1420070407776|user_6|
|blocked| campaign_2|   7|      youtube.com|ip_2|session_93|1420070412960|user_1|
|blocked| campaign_9| 153|       google.com|ip_3|session_22|1420070413824|user_9|
|blocked| campaign_8| 140|comedycentral.com|ip_8| session_4|1420070414688|user_2|
|blocked|campaign_19|  23|       amazon.com|ip_5|session_85|1420070415552|user_7|
+-------+-----------+----+-----------------+----+----------+-------------+------+

-------------------------------------------
Batch: 0
-------------------------------------------
+-----------------+-----+
|           domain|count|
+-----------------+-----+
|      nytimes.com|    1|
|      youtube.com|    2|
|        zynga.com|    1|
|       google.com|    2|
|        yahoo.com|    1|
|     facebook.com|    1|
|          cnn.com|    1|
|    wikipedia.org|    3|
|       sfgate.com|    1|
|       amazon.com|    2|
|   funnyordie.com|    1|
|sf.craigslist.org|    2|
|comedycentral.com|    2|
+-----------------+-----+

```

**=>  Can you explain how `append` mode works for query2? **  

**=>  Hit Ctrl+C  on terminal #1 to kill Spark streaming application**
