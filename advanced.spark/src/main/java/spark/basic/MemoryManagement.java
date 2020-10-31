package spark.basic;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.SparkSession;

import com.google.common.io.ByteStreams;

import scala.Tuple2;

@SuppressWarnings("serial")
class GetCsvRow implements FlatMapFunction<PortableDataStream, String> {
	public Iterator<String> call(PortableDataStream portableDataStream) throws Exception {
		ZipInputStream zipInputStream = new ZipInputStream(portableDataStream.open());
		@SuppressWarnings("unused")
		ZipEntry zipEntry = zipInputStream.getNextEntry();
		byte[] byteData = ByteStreams.toByteArray(zipInputStream);
		InputStreamReader inputStreamReader = new InputStreamReader(new ByteArrayInputStream(byteData));
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String singleLine = "";
		List<String> csvData = new ArrayList<String>();
		while ((singleLine = bufferedReader.readLine()) != null) {
			csvData.add(singleLine);
		}
		return csvData.iterator();
	}
}

/**
 * This class demonstrates the memory management in spark
 *
 */
public class MemoryManagement {

	public static void main(String[] args) {

		// TODO:change the sparkMaster to the master you are running
		String sparkMaster = "local";

		// TODO: change the spark.executor.memory to 512m to get out memory error and
		// set to 2g or 4g to resolve the error
		SparkSession sparkSession = SparkSession.builder().appName("Spark Memory management")
				.config("spark.executor.memory", "4g").master(sparkMaster).getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

		javaSparkContext.setLogLevel("ERROR");
		System.out.println(javaSparkContext.getConf().toDebugString());
		System.out.println("Started processing...");

		// TODO:specify the absolute zip file path here
		String zipFilePath = "***Absolute zip file path***";

		// Reading Zip file into java pair rdd
		JavaPairRDD<String, PortableDataStream> binDataPairRdd = javaSparkContext.binaryFiles(zipFilePath,
				javaSparkContext.defaultMinPartitions());

		// Getting portable stream rdd from pair rdd
		@SuppressWarnings("serial")
		JavaRDD<PortableDataStream> portableStreamRdd = binDataPairRdd
				.map(new Function<Tuple2<String, PortableDataStream>, PortableDataStream>() {
					public PortableDataStream call(Tuple2<String, PortableDataStream> stream) throws Exception {
						return stream._2;
					}
				});

		// Getting Csv data from the portable stream
		JavaRDD<String> csvRows = portableStreamRdd.flatMap(new GetCsvRow());
		// Finding count of rows in csv
		long rowsCount = csvRows.count();
		System.out.println("Number of rows in the file:" + rowsCount);
		javaSparkContext.close();
	}
}
