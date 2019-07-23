<link rel='stylesheet' href='../../assets/css/main.css'/>

[<< back to main index](../../README.md)

Lab 8.4 - Structured Streaming 3
=================================

### Overview
Run a Spark Structured Streaming job for word count problem using python

### Depends On
None

### Run time
30-40 mins


## STEP 1: Go to project directory
```bash
    $ cd ~/spark-labs/08-streaming/8.4-structured/python
```

## Step 2 : Fix TODO-1
Edit file : `structured-word-count.py`
Uncomment code block around TODO-1 (and only this one), so your code looks like this.  
Delete """  and """ around the TODO blocks

```bash
## TODO-1 : read from socket 10000
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10000) \
    .load()
# ----- end-TODO-1
```

## Step 3: Run Netcat Server to simulate network traffic (terminal #1)

Open another terminal into Spark node (terminal #2)

Use `nc` command to move text you type in terminal #1 to port 10000
Open an terminal and run this command at prompt

```bash
    $ nc -lk 10000

    # if this gives an error like 'Protocol not available' use this
    $  ~/bin/nc  -lk 10000
    
    # if this shows 'Port already in use', get the process is and kill the process
    $ sudo netstat -plnt | grep 10000
    # Process id will be shown in output
    $ sudo kill -9 <process id>
```

## Step 4: Run the streaming application

```bash
    # be in project root directory
    $ cd ~/spark-labs/08-streaming/8.4-structured/python

    $ ~/spark/bin/spark-submit  --master local[2]  --driver-class-path logging/ structured-word-count.py
```

Lets call this Terminal #2

Also note --master url `local[2]`
* We are using a local 'embedded' server  (quick for development)
* And we need at least 2 cpu cores -- one for receiver (long running task) and another for our program.  
If only allocated one core `local[1]`  the program will have run-time errors or won't run!

## Step 5:  Test by typing text in the terminal #1 (in netcat server)

In the Terminal #1, copy and paste the following lines (these are lines from our clickstream data)

```bash
hi scala
hi python
```

Inspect the output from Spark streaming on terminal #2

Output would be similar to the following:
```bash
-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
| word|count|
+-----+-----+
|scala|    1|
|   hi|    1|
+-----+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+------+-----+
|  word|count|
+------+-----+
| scala|    1|
|    hi|    2|
|python|    1|
+------+-----+

```

**=>  Hit Ctrl+C  on terminal #2 to kill Spark streaming application**
