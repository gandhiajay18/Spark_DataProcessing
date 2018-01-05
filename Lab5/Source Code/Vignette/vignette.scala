
val rawRdd = sc.textFile("titanic.csv")

rawRdd.count()

rawRdd.take(5)

val header = rawRdd.first()
val dataRdd = rawRdd.filter( _ != header)

dataRdd.first()

dataRdd.takeSample(false, 5, 0L)

val rowsRdd = dataRdd.map(line => line.split(",").map(_.trim))

rowsRdd.take(2)

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

def toVector( row : Array[String] ) : LabeledPoint = {
    val klass = row(1).charAt(1)-'0'.toDouble-1
    val age = if (row(2).contains("adults")) 1 else 0
    val sex = if (row(3).contains("women")) 1 else 0
    val survived = if (row(4).contains("yes")) 1 else 0
    LabeledPoint(survived, Vectors.dense(klass,age,sex))
}

val vectorsRdd = rowsRdd.map(row => toVector(row))

vectorsRdd.takeSample(false, 5, 0)

val splits = vectorsRdd.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

import org.apache.spark.mllib.tree.DecisionTree

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]((0,3), (1,2), (2,2))
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
                                         impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)


val testPassenger1 = LabeledPoint(0.0, Vectors.dense(0.0,1,0,0.0))
val testPassenger2 = LabeledPoint(0.0, Vectors.dense(1.0,1,0,0.0))
val testPassenger3 = LabeledPoint(0.0, Vectors.dense(2.0,1,0,0.0))
val testPassenger4 = LabeledPoint(1.0, Vectors.dense(0.0,0,0,1.0))

println(model.predict(testPassenger4.features))

import org.apache.spark.mllib.regression.LinearRegressionWithSGD

val numIterations = 100
val linearRegressionModel = LinearRegressionWithSGD.train(trainingData, numIterations)

// Compute raw scores on the test set.
val scoreAndLabels = testData.map { point =>
  val score = linearRegressionModel.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Area under ROC = " + auROC)