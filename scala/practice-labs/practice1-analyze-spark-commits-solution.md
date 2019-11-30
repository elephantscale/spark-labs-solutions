# 4.3 Practice Lab : Analyzing Spark commit logs (CSV)

Please see this blog post for solution and a screen cast.
http://elephantscale.com/2017/05/processing-csv-files-spark-2-part-1/

---

```bash
  $  ~/dev/spark/bin/spark-shell
```

An in Spark-shell, step by step

```scala

// default read
val commits = spark.read.csv("/data/spark-commits/sample.log")
commits.printSchema

// with header
val commits = spark.read.
              option("header", "true").
              csv("/data/spark-commits/sample.log")
commits.printSchema

// header + custom delimiter
val commits = spark.read.
              option("header", "true").
              option("delimiter", "|").
              csv("/data/spark-commits/sample.log")
commits.printSchema
commits.show(false)
commits.count

// ==== good to go!
// read the full file
val commits = spark.read.
              option("header", "true").
              option("delimiter", "|").
              csv("/data/spark-commits/spark-commits.log")
commits.count // 19001

// find commits from databricks
val db = commits.filter(commits("email").contains("databricks.com"))
db.count // 4116
db.show

// find top committers
commits.groupBy("email")
commits.groupBy("email").count
commits.groupBy("email").count.show
commits.groupBy("email").count.printSchema
// sort by desc
commits.groupBy("email").count.orderBy(desc("count"))
commits.groupBy("email").count.orderBy(desc("count")).show
// show top-10
commits.groupBy("email").count.orderBy(desc("count")).show(10)

// +--------------------+-----+
// | email              |count|
// +--------------------+-----+
// |matei@eecs.berkel...| 1341|
// | pwendell@gmail.com |  791|
// | rxin@databricks.com|  700|
// |tathagata.das1565...|  502|
// | rxin@apache.org    |  430|
// -----


```


### Solution one liner
```scala
val commits = spark.read.
              option("header", "true").
              option("delimiter", "|").
              csv("/data/spark-commits/spark-commits.csv")

commits.groupBy("email").count.orderBy(desc("count")).show(10)

// alternative

commitDS.groupBy($"committer").count.orderBy($"count".desc).withColumnRenamed("count","Count").show(1)
```


### Solution using sql
```scala

val commits = spark.read.
              option("header", "true").
              option("delimiter", "|").
              csv("/data/spark-commits/spark-commits.csv")

 commits.createOrReplaceTempView("commits")

 spark.catalog.listTables.show

 spark.sql("select * from commits").show

 -- find commits from databricks.com
 spark.sql("select * from commits where email like '%databricks.com'").show

 spark.sql("select count(*) from commits where email like '%databricks.com'").show

 -- find max commits
 spark.sql("select email, count(*) as total  from commits group by email order by total desc limit 10").show

// output
// +--------------------+-----+
// |               email|total|
// +--------------------+-----+
// |matei@eecs.berkel...| 1341|
// |  pwendell@gmail.com|  791|
// | rxin@databricks.com|  700|
// |tathagata.das1565...|  502|
// |     rxin@apache.org|  430|
// |davies@databricks...|  389|
// | meng@databricks.com|  338|
// |  sowen@cloudera.com|  332|
// |joshrosen@databri...|  311|
// |wenchen@databrick...|  282|
// +--------------------+-----+

```
