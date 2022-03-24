import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors


// Loads data.
val dataset = spark.read.
              option("header", "true").
              option("inferschema", "true").
              csv("../data/cars/mtcars_header.csv")
dataset.show(32) // 32 data points, show all

// extract the columns we need
val dataset2 = dataset.select("model", "mpg", "cyl")
dataset2.show


val assembler = new VectorAssembler().setInputCols(Array("mpg", "cyl")).setOutputCol("features")

val featureVector = assembler.transform(dataset2)
featureVector.show

// Trains a k-means model.
// k=2, iterations=10
val kmeans = new KMeans().setK(2).setMaxIter(10)
val model = kmeans.fit(featureVector)

// Evaluate clustering by computing Within Set Sum of Squared Errors.
val WSSSE = model.computeCost(featureVector)
println(s"Within Set Sum of Squared Errors = $WSSSE")

println("Cluster Centers: ")
model.clusterCenters.foreach(println)


// Print results
val predicted = model.transform(featureVector)

// print sorted by 'prediction'
predicted.sort("prediction").show(32,false)
predicted.sort("prediction", "mpg").show(32,false)

// lets count cars in each group
predicted.groupBy("prediction").count.show


// iterate over k
for (k <- 2 to 10) {
    //println (k)
    val kmeans = new KMeans().setK(k)setMaxIter(10)
    val model = kmeans.fit(featureVector)
    val WSSSE = model.computeCost(featureVector)
    //println(WSSSE)
    println("%d,%f".format(k,WSSSE))
}
