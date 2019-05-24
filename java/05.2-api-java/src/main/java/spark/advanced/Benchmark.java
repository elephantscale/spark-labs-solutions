package spark.advanced;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * to invoke:
 */
public class Benchmark {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Process Files -- MYNAME")
				.master("local")
				.getOrCreate();
		List<String> files = Arrays.asList("s3n://elephantscale-public/data/twinkle/1M.data",
				"s3n://elephantscale-public/data/twinkle/10M.data",
				"s3n://elephantscale-public/data/twinkle/100M.data",
				"s3n://elephantscale-public/data/twinkle/500M.data",
				"s3n://elephantscale-public/data/twinkle/1G.data",
				"s3n://elephantscale-public/data/twinkle/2G.data");
		for(String file : files) {
			System.out.println("### processing file : " + file);
			// count without caching
			Dataset<String> dataSet = spark.read().textFile(file);
			for(int i = 0; i < 5; i++) {
				Long t1 = System.nanoTime();
				Long count = dataSet.count();
				Long t2 = System.nanoTime();
				System.out.println (String.format("### %s : count (%,d) before caching took %,d ms",file, count, (t2-t1)/1000000 ));
			}
			dataSet.cache();
			for(int i = 0; i < 5; i++) {
				Long t1 = System.nanoTime();
				Long count = dataSet.count();
				Long t2 = System.nanoTime();
				System.out.println (String.format("### %s : count (%,d) after caching took %,d ms",file, count, (t2-t1)/1000000 ));
			}
		}
	}

}
