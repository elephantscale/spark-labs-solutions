/*
Usage:
spark-submit --class 'spark.advanced.ProcessFiles' --master spark://localhost:7077  target/spark.advanced-0.0.1-SNAPSHOT-jar-with-dependencies.jar    <files to process>

Multiple files can be specified
file to process can be :  /etc/hosts
                          scripts/1M.data
                          s3n://elephantscale-public/data/twinkle/100M.data
                          tachyon://tachyon_ip_address:19998/file

e.g:
- with 4G executor memory and turning off verbose logging
    ~/spark/bin/spark-submit --class 'spark.advanced.ProcessFiles'  --master local[*] --executor-memory 4g  --driver-class-path logging/   target/spark.advanced-2.11-jar-with-dependencies.jar   /data/text/twinkle/1G.data

 */

package spark.advanced;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
public class ProcessFiles {

	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println ("Need file(s) to load");
			System.exit(1);
		}
		// ## TODO 1 : give a name
		SparkSession spark = SparkSession.builder().appName("Process Files -- ES-Solutions").getOrCreate();
		for(String aFile : args) {
			// looping over files

			// ## TODO 2 : create an RDD out of file 
			//    hint :  spark.read().textFile(file)
			Dataset<String> fileDataSet = spark.read().textFile(aFile);
			Long t1 = System.nanoTime();

			// ## TODO 3 : count # of elements in RDD
			Long count = fileDataSet.count();
			Long t2 = System.nanoTime();
			System.out.println (String.format("### %s : count: %,d, Time taken: %,d ms",aFile, count, (t2-t1)/1000000 ));
			spark.stop(); // close the session
		}
	}

}
