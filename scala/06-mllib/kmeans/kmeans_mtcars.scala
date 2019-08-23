import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

// Load data
val dataset = spark.read.option("header", "true").
              option("inferschema", "true").
              csv("/data/cars/mtcars_header.csv")

// ## TODO-1 : print schema and display the dataset
dataset.printSchema
dataset.show

// ## TODO-2 : extract the columns we need : model, mpg and cyl
val dataset2 = dataset.select("model", "mpg", "cyl")

// ## TODO-3 : display dataset2
dataset2.show

val assembler = new VectorAssembler().
                    setInputCols(Array("mpg", "cyl")).
                    setOutputCol("features")
val featureVector = assembler.transform(dataset2)

// ## TODO-4 : display featureVector, what attributes do you see?
featureVector.show(40)
// if you only getting partial data use `featureVector.show(40)`

// Trains a k-means model.
// ## TODO-5 : set K = 2, maxIterations to 10
val kmeans = new KMeans().setK(2).setMaxIter(10)
val model = kmeans.fit(featureVector)

// Evaluate clustering by computing Within Set Sum of Squared Errors.
val WSSSE = model.computeCost(featureVector)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

// predict clusters
val predicted = model.transform(featureVector)
predicted.show

// print sorted by 'prediction'
predicted.sort("prediction").show(32,false)
predicted.sort("prediction", "mpg").show(32,false)

// lets count cars in each group
predicted.groupBy("prediction").count.show

// iterate over k
for (k <- 2 to 32) {
    //println (k)
    val kmeans = new KMeans().setK(k)setMaxIter(10)
    val model = kmeans.fit(featureVector)
    val WSSSE = model.computeCost(featureVector)
    //println(WSSSE)
    println("%d,%f".format(k,WSSSE))
}
