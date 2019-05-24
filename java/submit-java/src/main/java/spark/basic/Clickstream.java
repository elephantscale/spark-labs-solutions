package spark.basic;

import org.apache.spark.sql.SparkSession;

/**
Class that process JSON clickstream data
<br>
Usage:
spark-submit --class 'spark.basic.Clickstream' --master spark://localhost:7077 target/spark.basic-0.0.1-SNAPSHOT-jar-with-dependencies.jar  <files to process>

*/
public class Clickstream {

	public static void main(String[] args) {
		if(args.length == 1) {
			String file = args[0];// can be wildcard  'dir/*.log'
			
			// Give a name
			SparkSession spark = SparkSession.builder().appName("Process Files --MyName").master("local").getOrCreate();
			System.out.println ("### Ahoy mate, fix TODO items!");

			// ## TODO-1 : create an input dataframe;
			// hint : spark.read().json(input)
			/*Dataset<Row> clickStream = ???;*/

			// TODO-2  : count how many records we have
			/*Long numberOfRecords = clickStream.???;
			System.out.println("### Total clickstream records " + numberOfRecords);*/

			// TODO-3 : count traffic per domain
			/*Dataset<Row> domainCount = clickStream.groupBy("????").count();
			System.out.println("### Domain count: ");*/
			// display domainCount (hint : show)
			/*domainCount.???;*/
		} else {
			System.out.println ("Need a single argument(file path) to load\nIt can also be a wildcard  \"dir/*.json\"");
			System.exit(1);
		}
	}

}
