package spark.basic;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Concept:
 * 
 * In general, since your data are distributed among many nodes, 
 * they have to be shuffled before a join that causes significant network I/O and slow performance. 
 * 
 * In cases if we need to join large tables(fact tables) with smaller ones(dimension tables),
 * instead of sending large table data over network, we can just broadcast the smaller table to
 * all nodes to perform a map-side/broadcast join.
 * 
 * Using broadcast variables which can be shared across nodes 
 * ------------------------------------------IMPORTANT----------------------------------------
 * Samples Used:(In this project directory)
 * 1. store_locations.json
 * 2. us_states.json
 *---------------------------------------------------------------------------------------------
 */
public class MapSideJoin {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Spark - MapSideJoin")
				.master("local[*]")
				.getOrCreate();

		JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
		System.out.println(sparkContext.getConf().toDebugString());

		//Dataframe containing US states details 
		Dataset<Row> stateDf = spark.read()
				.json("/data/joins/us_states.json");

		// Create a DataFrame based on the store locations.
		Dataset<Row> storesDF = spark.read()
				.json("/data/joins/store_locations.json");

		// ## TODO 1 - Set stateDf as a broadcast variable so that,
		// it will be shared across the nodes in the cluster
		// Hint broadcast method available in sparkcontext
		// Create a DataFrame of US state data with the broadcast variables.
		
		//Broadcast<???> stateDFBroadcasted = ??????;

		// More hint
		// Broadcast<Dataset<Row>> stateDFBroadcasted = sparkContext.broadcast(stateDf);

		// Join the DataFrames to get an aggregate count of stores in each US Region
		System.out.println("###Number of stores in each US region :");
		/*Dataset<Row> joinedDF = storesDF.join(stateDFBroadcasted.value(), "state")
				.groupBy("census_region")
				.count();		
		joinedDF.show();*/
		spark.close();
		sparkContext.close();
	}
}
