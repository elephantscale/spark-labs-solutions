<link rel='stylesheet' href='assets/css/main.css'/>

# Spark Labs - Scala

Welcome to Spark labs bundle.  This is the Scala track.

## To Instructor

Create a lab bundle as follows

```bash
    $   ./assemble-labs.sh
```

# Labs

Instructor will provide lab bundle

# Download Data

The VMs already have data loaded.  This for your own reference.

[Link to Full Dataset](https://s3.amazonaws.com/elephantscale-public/data/data.zip)
(Note: Large download, ~300 Meg)

- Click the above link to download or
- use `wget` from command line

```
    $    wget   "https://s3.amazonaws.com/elephantscale-public/data/data.zip"
```

# Labs

### 1 - Scala Primer

- [1.1 - Scala shell](01-scala/README.md)
- [1.2 - Functions]
- [Setup 1](setup1.md) - Instructor to demo first
- [1.3 - File IO](01-scala/1.3-file.md)
- [1.4 - Higher Order Functions](01-scala/1.4-functions.md)
- [1.5 - Vending Machine class](01-scala/vending-machine/1.5-README.md)
- [1.6 - Unit testing with SPECS2](01-scala/vending-machine/1.6-SPECS-README.md)

### 2 - Spark Intro

- [2.1 - Install and run Spark](02-intro/2.1-install-spark-scala.md)
- 2.2 - Spark Shell - [Standalone](02-intro/2.2-shell-scala.md)  || [Hadoop version](02-intro/2.2H-spark-shell-hadoop.md)

### 3 - Spark Core

- [3.1 - RDD basics](03-rdd/3.1-rdd-basics-scala.md)
- [3.2 - Dataset basics](03-rdd/3.2-dataset-basics-scala.md)
- [3.3 - Caching](03-rdd/3.3-caching-scala.md)

### 4 - Dataframes and Datasets

- [4.1 - Dataframes](04-dataframe/4.1-dataframe-scala.md)
- [4.2 - Spark SQL ](04-dataframe/4.2-sql-scala.md)
- [4.3 - Dataset](04-dataframe/4.3-dataset-scala.md)
- [4.4 - Caching 2 SQL](04-dataframe/4.4-caching-2-sql-scala.md)
- [4.5 - Spark & Hive (Hadoop)](04-dataframe/4.5-spark-and-hive-scala.md)
- [4.6 - Data formats](04-dataframe/4.6-data-formats-scala.md)


### 5 - API

- [5.1 - Submit first application](05-api/5.1-submit-scala.md)
- BONUS :  [5.2 - Mapreduce using API](05-api/5.2-mapreduce-scala.md)

### Practice Labs for end of day 2

- [Practice Lab 1 - Analyze Spark Commit logs](practice-labs/commit-logs-scala.md)
- (If time permits) [Practice Lab 3 - Optimize SQL query](practice-labs/optimize-query-scala.md)


### 6 - MLLib

- [6.1 - Kmeans](06-mllib/kmeans/kmeans-scala.md)
- [6.2 - Recommendations](06-mllib/recs/README.md)
- [6.3 - Classification](06-mllib/classification/README.md)

### 7 - GraphX

- [7.1  - Influencers (Twitter)](07-graphx/7.1-influencer-scala.md)
- [7.2  - Shortest path (in LinkedIn)](07-graphx/7.2-shortest-path-scala.md)

### 8 - Streaming

#### Structured Streaming 

This is the new recommended API for streaming.  

- [Structured Streaming 1 - Intro](08-streaming/8.4-structured/README1-intro-scala.md)
- [Structured Streaming 2 - Word Count](08-streaming/8.4-structured/README2-wordcount-scala.md)
- [Structured Streaming 3 - Clickstream](08-streaming/8.4-structured/README3-clickstream-scala.md)

#### Classic Streaming 

- [Streaming over TCP](08-streaming/8.1-over-tcp/README-scala.md)
- [Windowed Count](08-streaming/8.2-window/README-scala.md)
- [Kafka Streaming](08-streaming/8.3-kafka/README-scala.md)


### 9 - Operations

- [9.1 - Cluster setup](09-ops/9.1-cluster-setup.md)

### 10 - Spark and Hadoop (all the Hadoop labs are grouped here)

- [2.2H - Spark Shell on Hadoop](02-intro/2.2H-spark-shell-hadoop.md)
- [3.1 - Loading RDDs from HDFS](03-rdd/3.1-rdd-basics-scala.md)
- [4.2 - Spark SQL on Hadoop](04-dataframe/4.2-sql-scala.md)
- [4.4H - Spark & Hive](04-dataframe/4.5-spark-and-hive-scala.md)


### Practice Labs

- [Practice Lab 1 - Analyze Spark Commit logs](practice-labs/commit-logs-scala.md)
- [Practice Lab 2 - Analyze house sales data](practice-labs/house-sales-scala.md)
- [Practice Lab 3 - Optimize SQL query](practice-labs/optimize-query-scala.md)
- [Practice Lab 4 - Analyze clickstream data](practice-labs/clickstream-scala.md)
