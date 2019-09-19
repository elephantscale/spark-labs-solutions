## Classification solution 

```scala
// This solves the classification task using an SVM (Support Vector Machine), a linear
// method for binary classification.

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD;

// Input file:
//0:"",1:"state",2:"account_length",3:"area_code",4:"international_plan",5:"voice_mail_plan",
//6:"number_vmail_messages",7:"total_day_minutes",8:"total_day_calls",9:"total_day_charge",
//10:"total_eve_minutes",11:"total_eve_calls",12:"total_eve_charge",13:"total_night_minutes",
//14:"total_night_calls",15:"total_night_charge",16:"total_intl_minutes",17:"total_intl_calls",
//18:"total_intl_charge",19:"number_customer_service_calls",20:"churn"

def parseData(vals : RDD[String]) : RDD[LabeledPoint] = {
  vals.map { s =>
    val parts = s.split(',').drop(4).map(_.toDouble);
    LabeledPoint(if (parts(16)==1) 1.0 else 0.0, Vectors.dense(parts))
  }
}

// Load and parse the data
val trainingData = sc.textFile("../../../data/churn/churntrain.csv")
val splitTrainingData = parseData(trainingData) 

val testData = sc.textFile("../../../data/churn/churntest.csv")
val splitTestData = parseData(testData) 

// Run training algorithm to build the model
val numIterations = 100
val model = SVMWithSGD.train(splitTrainingData, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set. 
val scoreAndLabels = splitTestData.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Area under ROC = " + auROC)
```
