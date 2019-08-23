import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

def parseData(vals : RDD[String]) : RDD[Rating] = {
  vals.map(_.split(',') match { case Array(user, item, rating) =>
    Rating(user.toInt, item.toInt, rating.toDouble)
  })
}

// For the dating website 
// Users = Users
// Items = other users

// ratings.dat is the dating website
val data = sc.textFile("/data/dating/medium/ratings.dat")
val ratings = parseData(data)
val model = ALS.train(ratings, rank = 10, iterations = 5, 0.01)

// ratings.take(3).foreach(println)

// Get rid of rating to test model's effectiveness
// TODO: TRANSFORM Rating -> Tuple of (user, item)
// (i.e., get rid of the rating)

val userItems = ratings.map { case Rating(user, item, rating) => 
  (user, item)}

// Do a test prediction
// TODO call model.predict() on userItems, and then map the output of that 
// to (user, item), rating

val predict = model.predict(userItems)
val recs = predict.map { case Rating(user, item, rating) => 
  ((user, item), rating)}

val ratingsAndRecs = ratings.map { case Rating(user, item, rating) => 
  ((user, item), rating)
}.join(recs)

val MSE = ratingsAndRecs.map { case ((user, item), (r1, r2)) => 
  val err = (r1 - r2)
  err * err
}.mean()

println("Mean Squared Error = " + MSE)

val recsForEachUser = recs.map { case ((user, item), rating) => (user, item, rating) 
  }.collect.groupBy(_._1).mapValues(_.sortBy(- _._3).take(4)).mapValues(_.map(_._2))

/*
// ### Step 4: Running on some of your own data
// Note: Comment all the previous code other than import statements before running this code
val personaldata = sc.textFile("personalratings.txt")
val personalratings = personaldata.map(_.split(',') match { case Array(user, item, rating) =>
    Rating(user.toInt, item.toInt, rating.toDouble)
  })
println(personalratings.take(15).foreach(println))
val model = ALS.train(personalratings, rank = 10, iterations = 5, 0.01)
val userItems = personalratings.map { case Rating(user, item, rating) => 
  (user, item)}
val predict = model.predict(userItems)
val recs = predict.map { case Rating(user, item, rating) => 
  ((user, item), rating)}

val ratingsAndRecs = personalratings.map { case Rating(user, item, rating) => 
  ((user, item), rating)
}.join(recs)

val MSE = ratingsAndRecs.map { case ((user, item), (r1, r2)) => 
  val err = (r1 - r2)
  err * err
}.mean()s

println("Mean Squared Error = " + MSE)

val recsForEachUser = recs.map { case ((user, item), rating) => (user, item, rating) 
  }.collect.groupBy(_._1).mapValues(_.sortBy(- _._3).take(4)).mapValues(_.map(_._2))
*/