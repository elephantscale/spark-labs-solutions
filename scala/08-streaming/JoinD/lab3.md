[<< back to main index](../README.md) / [Streaming labs](./README.md)

Lab 3 - Spark Streaming - This lab shows you how to join multiple labs  
==================================

### Overview
One of the most useful features of Spark Streaming is your ability to join multiple DSTreams based on the common key pairs. 
This lab gives you the Join operator example. 

### Depends On 
None

### Run time
10-20 mins


---------------------
STEP 1: Edit source file
---------------------
Go to the project root directory
```bash
$    cd ~/dev/spark-labs/streaming/lab3
```

**edit file : `src/main/scala/join.scala`**  
**And fix the TODO items**

```
$    vi  src/main/scala/Joind.scala
# or 
$    nano  src/main/scala/Joind.scala
```


--------------------------
STEP 2: Compile the project
--------------------------
We will use `sbt` to build the project.  

**Inspect the `build.sbt` file**
```bash
$  cat   build.sbt
```

The file will look follows:
```scala
// blank lines are important!

name := “Join DStreams“

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.3.1" % "provided"
)

```

```bash
$   sbt package
# to do a clean rebuild use
#  sbt clean package
```

Make sure there are no errors and there is output in `target` dir.
```bash
$  ls -l   target/scala-2.10
```
You should see output like follows
```
drwxr-xr-x  3 vsistla  staff   102B Apr 16 09:59 classes/
-rw-r--r--  1 vsistla  staff    13K Apr 16 09:59 joind_2.10-1.0.jar
```

`joind_2.10-1.0.jar `  is our code compiled.
 
--------------------------
STEP 3: Run The Application
--------------------------
```
$  ~/dev/spark-labs/streaming/lab3/sbt run
```
Note - hope you have modified log4j.properties in conf folder to remove verbose. 

Lets call this Terminal #1

----------------
STEP 4: Run Netcat Server to send text through TCP connection.
----------------

Use nc command to move text you type in terminal to port 9999
Open an terminal and run this command at prompt

$ nc -lk 9999

Lets call this Terminal #2

-------------------------
STEP 5:  Test by copying the sample data from sample-join.csv file. Please note that this data is modified for this example. 

-------------------------

In the Terminal #2, copy and paste as many lines from sample-join.csv file and see the application display the 
IPs in Terminal #1


--------------------------
STEP 6: Also can print to a file in your file system
---------------------------

Comment and uncomment respective lines to test this. 

End. 
