Lab 1 : Solutions
-----------------
Launch Spark Server
```bash
$  cd ~/spark
$  ./sbin/start-all.sh
```

Launch Spark Shell
```bash
$  ./bin/spark-shell
```

Connect shell to server
```bash
$  ./bin/spark-shell  --master spark://localhost:7077
```

Add more memory
```bash
$  ./bin/spark-shell   --executor-memory 2G
```