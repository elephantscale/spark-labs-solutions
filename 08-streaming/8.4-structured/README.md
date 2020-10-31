# Spark Structured Streaming

## References

- https://github.com/apache/spark/tree/master/examples/src/main/python/sql/streaming
- https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

## Simulating source connections

     `ncat -l -k -p 10000`
Note: Netcat (nc) will not support multiple connections
      `nc -l -k -p 10000`
https://stackoverflow.com/questions/45618489/executing-separate-streaming-queries-in-spark-structured-streaming
