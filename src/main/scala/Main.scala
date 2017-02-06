import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.joda.time.DateTime

import scala.io.Source

/**
  * Created by rakyunkoh on 2017. 1. 12..
  */
object Main {

  def main(args: Array[String]): Unit = {

//    val sc = new SparkContext()
//
//    val data = sc.textFile("../labeled.csv")
//    val parsedData = data.map { line =>
//      val parts = line.split(',')
//
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
//    }.cache()
//
//    val numIterations = 100
//    val stepSize = 0.0000001
//    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
//
//    val valuesAndPreds = parsedData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//
//    val MSE = valuesAndPreds.map{ case(v,p) => math.pow((v - p), 2) }.mean()
//    println("trained Mean Squared Error = " + MSE)
//
//    model.save(sc, "SGDLinearRegressionModel")
//    val sameModel = LinearRegressionModel.load(sc, "SGDLinearRegressionModel")


    //일자 시작지점 및 종료지점 지정
    val start = new DateTime(2009, 10, 23, 0, 0)
    val end = new DateTime(2014, 10, 23, 0, 0)

    val files = new File("data/stocks/").listFiles()
    val rawStocks: Seq[Array[(DateTime, Double)]] =
      files.flatMap(file => {
        try {
          Some(readYahooHistory(file))
        } catch {
          case e : Exception => None
        }
      }).filter(_.size >= 260*5+10)

    val stocks: Seq[Array[(DateTime,Double)]] = rawStocks.map(trimToRegion(_, start, end)).map(fillInHistory(_,start,end))
    val factors: Seq[Array[(DateTime,Double)]] = (factors1 ++ factors2).map(trimToRegion(_, start, end)).map(fillInHistory(_,start,end))

    }

  def readInvestingDotComHistory(file: File):Array[(DateTime, Double)] = {
    val format = new SimpleDateFormat("MMM d, yyyy")
    val lines = Source.fromFile(file).getLines().toSeq
    lines.map(line => {
      val cols = line.split('\t')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray

  }

  def readYahooHistory(file:File):Array[(DateTime, Double)] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val lines = Source.fromFile(file).getLines().toSeq
    lines.tail.map(line => {
      val cols = line.split(',')
      val date = new DateTime(format.parse(cols(0)))
      val value = cols(1).toDouble
      (date, value)
    }).reverse.toArray
  }


  val factorsPrefix = "data/factors/"
  val factors1: Seq[Array[(DateTime, Double)]] =
    Array("crudeoil.tsv", "us30yeartreasurybonds.tsv").map{ x =>
      new File(factorsPrefix + x)
    }.map(readInvestingDotComHistory)

  val factors2: Seq[Array[(DateTime, Double)]] =
    Array("SNP.csv", "NDB.csv").map(x => new File(factorsPrefix + x)).map(readYahooHistory)

  def trimToRegion(history:Array[(DateTime, Double)], start:DateTime, end :DateTime) : Array[(DateTime, Double)] = {
    var trimmed = history.dropWhile(_._1 < start).takeWhile(_._1 <= end)

    if(trimmed.head._1 != start) {
      trimmed = Array((start, trimmed.head._2  )) ++ trimmed
    }

    if (trimmed.last._1 != end) {
      trimmed = trimmed ++ Array((end, trimmed.last._2))
    }

    trimmed

  }

  def fillInHistory(history:Array[(DateTime, Double)], start:DateTime, end:Datetime): Array[(DateTime, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(DateTime, Double)]()
    var curDate = start
    while (curDate < end) {
      if (cur.tail.nonEmpty && cur.tail.head._1 == curDate) {
        cur = cur.tail
      }
      filled += ((curDate, cur.head._2))

      curDate += 1.days
      if (curDate.dayOfMonth().get > 5) curDate += 2.days
    }
    filled.toArray
  }


}
