package spark.basic;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Managing and avoiding shuffles
 * Shuffle typically involves copying data across executors and machines, making it a complex and costly operation.
 * Here we will be loading sample datasets of netflix and we will be understanding the following,
 * 
 * 1. How to avoid unnecessary shuffling
 * 2. How it is useful when it is done
 * 
 * Note - Performance improvement can be only seen if the cluser has atleast 2 nodes, 
 * Since the data will reside on multiple nodes, so that data is shuffling.
 * 
 */
public class Shuffling {

	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkSession.builder().appName("Spark - Managing Shuffle")
				.master("local")
				.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		//System.out.println(sc.getConf().toDebugString());

		//## TODO-1: Repartition by required column(Pre shuffling) after reading csv itself
		// And persist the partition data
		// Hint: repartition with 2 partitions and persist it.
		Dataset<Row> userRatings = spark.read().option("header", true)
				.csv("/data/netflix/netflix-user-ratings-10k.csv").
						repartition(2);

		Dataset<Row> movieTitles = spark.read().option("header", true)
				.csv("/data/netflix/movie-titles.csv");

		// We are joining movieTitles and ratings to get total number of reviews by the users for a each movie
		// Here only our first shuffling takes place in userRatings since we are applying an action(join) here and
		// the shuffled data is grouped across partitions.
		// It will also persist the partition data(since we are applying it) so that further shuffling can be avoided.
		System.out.println("### Movies and their user reviews");
		Dataset<Row> moviesAndRatingsDf = movieTitles.join(userRatings,"MovieID");
		moviesAndRatingsDf.select("Title","MovieID")
		.groupBy("Title","MovieID").count()
		.orderBy("Title")
		.select(new Column("Title").alias("Movie Name"), new Column("count").alias("Total Reviews"))
		.show();

		// We are listing out the oscar won movies with its release year
		// Here shuffling is not performed since the df(userRatings) is already shuffled and partitioned
		// Performance is improved since shuffling does not take place
		Dataset<Row> movieOscars = spark.read().option("header", true)
				.csv("/data/netflix/movie-oscars.csv");
		movieOscars.createTempView("oscarDetailsTable");
		
		Dataset<Row> oscarMoviesOnDf = spark.sql("Select * from oscarDetailsTable where Oscar = true");
		Dataset<Row> oscarMoviesAndRatingCount = oscarMoviesOnDf.join(userRatings, "MovieId")
				.groupBy("MovieId").count();
		
		System.out.println("###Oscar won movies with its release year");
		oscarMoviesAndRatingCount.join(movieTitles,"MovieId")
		.orderBy("Title")
		.select(new Column("Title").alias("Movie Name(Oscar won)")
				, new Column("year").alias("Release Year")
				, new Column("count").alias("Total Reviews"))
		.show();
	}
}
