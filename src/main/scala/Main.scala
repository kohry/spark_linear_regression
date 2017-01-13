import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

/**
  * Created by rakyunkoh on 2017. 1. 12..
  */
object Main {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()

    val data = sc.textFile("../labeled.csv")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    val numIterations = 100
    val stepSize = 0.0000001
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map{ case(v,p) => math.pow((v - p), 2) }.mean()
    println("trained Mean Squared Error = " + MSE)

    model.save(sc, "SGDLinearRegressionModel")
    val sameModel = LinearRegressionModel.load(sc, "SGDLinearRegressionModel")

    }
}
