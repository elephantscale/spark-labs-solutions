<link rel='stylesheet' href='../../assets/css/main.css'/>

[<< back to main index](../../README.md)

Lab 8.1 - Spark Streaming to find all blocked IPs
=================================================

### Overview
Submit a job for Spark Streaming to find all the Blocked IP Addresses Over Socket/TCP

### Depends On
None

### Run time
30-40 mins


## STEP 1: Go to project directory
```bash
$ cd ~/spark-labs/08-streaming/8.1-over-tcp/python
```

## Step 2 : Fix TODO-1 and TODO-2
**Edit file : [over-tcp.py](over-tcp.py)**  

See [Edit Files](../../edit-files.md) section for help.

## Step 3: Run Netcat Server to start network traffic

Open another terminal into Spark node (terminal #2)

Use `nc` command to move text you type in terminal #2 to port 10000
Open an terminal and run this command at prompt

```bash
    $ nc -lk 10000

    # if this gives an error like 'Protocol not available' use this
    # $  ~/bin/nc  -lk 10000
    
    # if this shows 'Port already in use', get the process is and kill the process
    $ sudo netstat -plnt | grep 10000
    # Process id will be shown in output
    $ sudo kill -9 <process id>
```

## Step4: Run the streaming application

```bash
    # be in project root directory
    $  cd  ~/spark-labs/08-streaming/8.1-over-tcp/python
    $  ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/ over-tcp.py
```

Lets call this Terminal #1

Also note --master url `local[2]`
* We are using a local 'embedded' server  (quick for development)
* And we need at least 2 cpu cores -- one for receiver (long running task) and another for our program.  
If only allocated one core `local[1]`  the program will have run-time errors or won't run!

## Step5:  Test by typing text in the terminal

Enter some text in netcat terminal (terminal #2)

```
a
b
```

Inspect the output from Spark streaming (terminal #1)

You should see something similar to this screen shot.
(Click on the image for larger version)   

<a href="../../assets/images/8.1a.png"><img src="../../assets/images/8.1a.png" style="border: 5px solid grey; max-width:100%;"/></a>

**=>  Hit Ctrl+C  on terminal #1 to kill Spark streaming application**

## Step6: Filter (TODO-3)
**==>  Edit file :  `over-tcp.py`  **  
** ==> Uncomment block around TODO-3, to filter lines that has the word 'blocked'**

```bash
## TODO-3 : filter for lines that has 'blocked'
blocked= lines.filter(lambda line : "???" in line)
blocked.pprint()
```
**==> Run the code**

```bash
    $   cd ~/spark-labs/08-streaming/8.1-over-tcp  
```

Using 'netcat' program, send some data to streaming.  Make sure some data has the text 'blocked'

```console
a
b blocked
```

Output may look like this:

<a href="../../assets/images/8.1b"><img src="../../assets/images/8.1b.png" style="border: 5px solid grey; max-width:100%;"/></a>

## Step 8: Save data into files (TODO-4)

**==> Edit file : `over-tcp.py`**
**Uncomment TODO-4 code block so it looks like this**

```bash
## TODO-4 : save the output in a file
blocked.saveAsTextFiles("out-blocked")
```

**=> Run the program**
```bash
$   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  over-tcp.py
```

**=> Send some data through netcat window (terminal #2)**

**=> Hit Contrl+C in terminal #2 to terminate Spark streaming**

**=> Inspect the `out` directory**

```bash
$   ls -l
```

Output may look like this:

```console
drwxr-xr-x 4 sujee staff 136 Jan 22 22:42 out-blocked-1485153750000/
drwxr-xr-x 8 sujee staff 272 Jan 22 22:42 out-blocked-1485153760000/
drwxr-xr-x 6 sujee staff 204 Jan 22 22:42 out-blocked-1485153770000/
drwxr-xr-x 4 sujee staff 136 Jan 22 22:55 out-blocked-1485154510000/
drwxr-xr-x 4 sujee staff 136 Jan 22 22:55 out-blocked-1485154520000/
```

**=> Inspect some files, what do you see?**

```bash
# you may need to adjust the file name
$ find out-blocked*
```

Output may look like this:

```console
out-blocked-1485153760000/_SUCCESS
out-blocked-1485153760000/part-00000
out-blocked-1485153760000/part-00001
out-blocked-1485153770000
out-blocked-1485153770000/._SUCCESS.crc
out-blocked-1485153770000/.part-00000.crc
out-blocked-1485153770000/_SUCCESS
out-blocked-1485153770000/part-00000
```

Files
* part-0000 : this is data
* _SUCCESS_ : indicates that directory is complete
* crc : Checksum files

## Bonus Lab  1 : Extract BLOCKED IPs
**==> Edit file :  `over-tcp.py`**  

**==> Fix BONUS-LAB to extract blocked IPs**

**==> Run a program like this**

```bash
    $   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  over-tcp.py
```

**==> Test with this clickstream data, using netcat window**

```
1420070400000,ip_1,user_5,clicked,facebook.com,campaign_6,139,session_98
1420070400864,ip_2,user_3,viewed,facebook.com,campaign_4,35,session_98
1420070401728,ip_8,user_8,clicked,youtube.com,campaign_12,115,session_92
1420070402592,ip_1,user_2,blocked,wikipedia.org,campaign_5,129,session_91
1420070403456,ip_7,user_7,viewed,funnyordie.com,campaign_11,12,session_13
```

## Bonus Lab  2 : Network Word Count
**==> Inspect file : [networkwordcount.py](networkwordcount.py)**

**==> Run a program like this**
```bash
    $   ~/spark/bin/spark-submit  --master local[2]   --driver-class-path logging/  networkwordcount.py
```

**==> Test with data using NC**
```bash
$  nc -lk 10000
a b c
a
b c
d
```
