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

val data = sc.textFile("../../data/dating/medium/ratings.dat")

val ratings = parseData(data)

// ALS.train (rdd, rank, iterations, lambda)
//    - rank : how many features to use
//    - lambda : regularization factor (recommended: 0.01)
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.recommendation.ALS$
val model = ALS.train(ratings, rank = 10, iterations = 5, 0.01)

val usersItems = ratings.map { case Rating(user, item, rating) =>
  (user, item)
}


val recs = 
  model.predict(usersItems).map { case Rating(user, item, rating) => 
    ((user, item), rating)
  }

val ratingsAndRecs = ratings.map { case Rating(user, item, rating) => 
  ((user, item), rating)
}.join(recs)
val MSE = ratingsAndRecs.map { case ((user, item), (r1, r2)) => 
  val err = (r1 - r2)
  err * err
}.mean()
println("Mean Squared Error = " + MSE)

val personaldata = sc.textFile("personalratings.txt")

val personalratings = parseData(personaldata)

val recsForEachUser = recs.map { case ((user, item), rating) => (user, item, rating) 
  }.collect.groupBy(_._1).mapValues(_.sortBy(- _._3).take(4)).mapValues(_.map(_._2))

exit // exit shell
