<link rel='stylesheet' href='../assets/css/main.css'/>

Spark Job Submission
==================================

### Overview
Submit a Java job to Spark

### Depends On
None

### Run time
20-30 mins


## STEP 1: Edit source file

Go to the project directory

```bash
    $    cd ~/dev/spark-labs/05-api-java
```


**=> Edit file : `~/dev/spark-labs/05-api-java-java/src/main/java/spark/advanced/ProcessFiles.java`**  
**=> And fix the TODO items**


## STEP 2: Compile the project

We will use `maven` to build the project.  

**=> Inspect the `pom.xml` file**


**=> Kick off a build**  
(This will take a few minutes for the first time you run it)

```bash
    # be in the project root level directory
    $   cd <our-project>

    $   mvn package

    # to do a clean rebuild use
    $  mvn clean package
```

Make sure there are no errors and there is output in `target` dir.

```bash
    $  ls -l   target/
```

You should see the "*-with-dependencies.jar"

## STEP 3: Test Application in Local Master Mode

```bash
    $  cd  ~/dev/spark-labs/05-api-java-java

    $   ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master local[*]  target/spark.basic-2.11-jar-with-dependencies.jar    README.md
```

**==> Checkout the Shell UI (4040)**   

**==> Hit Enter key to terminate the program**

**==> Turn off the logs by sending logs by `2> logs` **   

```bash
    $   ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master local[*]  target/spark.basic-2.11-jar-with-dependencies.jar    README.md  2> logs
```

**==> Try a few input files **
```bash
    $   ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master local[*]  target/spark.basic-2.11-jar-with-dependencies.jar    /data/text/twinkle/*  2> logs
```


Now let's submit the application to Spark server

## STEP 4: Start Spark Server

```bash
    $  ~/dev/spark/sbin/start-all.sh
```

**=> Check the Spark Server UI at port 8080.**  
**=> Note the Spark master URL.**  

<img src="../assets/images/4.1b.png" style="border: 5px solid grey; max-width:100%;"/>


## STEP 5: Submit the application

Use the following command to submit the job

```bash
    $  cd  ~/dev/spark-labs/05-api-java

    # single README file
    $   ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master MASTER_URL  target/spark.basic-2.11-jar-with-dependencies.jar    README.md   2> logs

    # multiple twinkle files
    $   ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master MASTER_URL  target/spark.basic-2.11-jar-with-dependencies.jar    /data/text/twinkle/*  2> logs
```

* MASTER URL : substitute your spark master url
* for files you can try `README.md`

**=> Watch the console output**

It may look like this

```console
   2019-01-23 15:38:44 INFO  DAGScheduler:54 - Job 0 finished: count at ProcessFiles.java:22, took 3.245631 s
 ###  README.MD : count: 61, Timetook: 4,838 ms 
 ### Hit enter to terminate the program...:

```
The lines starting with `###` are output from our program


## STEP 6:  Configuring logging

#### To quickly turn off the logging:
Redirect the logs as follows `  2> logs`.   
All logs will be sent to `logs` file.  
```bash
    $  ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master MASTER_URL  target/spark.basic-2.11-jar-with-dependencies.jar    <files to process>    2>  logs
```

#### Using log4j config
Spark by default logs at INFO level.  While there is a lot of useful information there, it can be quite verbose.

We have a `logging/log4j.properties` file.  Inspect this file

```bash
    $    cat   logging/log4j.properties
```


The file has following contents

```
# Set everything to be logged to the console
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Settings to quiet third party logs that are too verbose
log4j.logger.org.eclipse.jetty=WARN
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO

## set log levels for our loggers
## everything under 'x' package
#log4j.logger.x=INFO
## specific file
#log4j.logger.spark.basic.ProcessFiles=DEBUG
```



We provide `--driver-class-path logging/`  to spark-submit to turn off logging

Here is an example

```bash
    $   ~/dev/spark/bin/spark-submit --class 'spark.basic.ProcessFiles' --master local[*]  --driver-class-path logging/  target/spark.basic-2.11-jar-with-dependencies.jar    README.md
```
