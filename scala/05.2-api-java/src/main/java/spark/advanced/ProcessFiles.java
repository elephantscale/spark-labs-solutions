package spark.advanced;

import org.apache.spark.sql.SparkSession;


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
    spark-submit --class 'spark.advanced.ProcessFiles'  --master spark://localhost:7077 --executor-memory 4g  --driver-class-path logging/   target/spark.advanced-0.0.1-SNAPSHOT-jar-with-dependencies.jar   s3n:///elephantscale-public/data/twinkle/1G.data

 */
public class ProcessFiles {

	public static void main(String[] args) {
		if(args.length < 1) {
			System.out.println ("Need file(s) to load");
			System.exit(1);
		}
		// ## TODO 1 : give a name
		SparkSession spark = SparkSession.builder().appName("Process Files --MyName").getOrCreate();
		for(String aFile : args) {
			// looping over files

			// ## TODO 2 : create an RDD out of file 
			//    hint :  spark.read().textFile(file)
			/*Dataset<String> fileDataSet = ???;
			Long t1 = System.nanoTime();

			// ## TODO 3 : count # of elements in RDD
			Long count = ???;
			Long t2 = System.nanoTime();
			System.out.println (String.format("### %s : count: %,d, Timetook: %,d ms",aFile, count, (t2-t1)/1000000 ));
			 */
			
			// HACK : so the 4040 UI stays alive :-)
			System.out.println("### Hit enter to terminate the program...:");
			String line = System.console().readLine();
			spark.stop(); // close the session
		}
	}

}
