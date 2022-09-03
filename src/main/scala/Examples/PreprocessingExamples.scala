package Examples

import Preprocessing.{MaxAbsScaler, MinMaxScaler, StandardScaler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{mean, stddev}

object PreprocessingExamples {

  val spark: SparkSession = SparkSession.builder()
    .appName("PreprocessingStandardScaler")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // Set in order to make sure Exception does not occur due to string concatenation
  spark.conf.set("spark.debug.maxToStringFields", 1000)

  def testZeroMeanUnitVar(df: DataFrame, colName: Array[String]): Unit =
    colName.foreach { col =>
      println(s"$col mean: ${df.select(mean(col)).head().getDouble(0)}")
      println(s"$col stddev: ${df.select(stddev(col)).head().getDouble(0)}")
    }

  def demoStandardScaler(): Unit = {
    val aDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .csv("src/data/usedcars_dataset.csv")

    val featureCols = List("compression_ratio", "horsepower", "peak_rpm", "city_mpg", "highway_mpg", "price")

    // StandardScaler section
    val stdScaler = new StandardScaler(aDF, featureCols)
    val transformedDF1 = stdScaler.transform()
    transformedDF1.printSchema()
    transformedDF1.show()

    // Test for zero mean and unit variance
    testZeroMeanUnitVar(transformedDF1, transformedDF1.columns)

    // MinMaxScaler section
    val minMaxScaler = new MinMaxScaler(aDF, featureCols, Option((2.0,3.0)))
    val transformedDF2 = minMaxScaler.fitTransform()
    transformedDF2.printSchema()
    transformedDF2.show()

    // Test for zero mean and unit variance
    testZeroMeanUnitVar(transformedDF2, transformedDF2.columns)

    // MaxAbsScaler section
    val maxAbsScaler = new MaxAbsScaler(aDF, featureCols)
    val transformedDF3 = maxAbsScaler.fitTransform()
    transformedDF3.printSchema()
    transformedDF3.show()

    // Test for zero mean and unit variance
    testZeroMeanUnitVar(transformedDF3, transformedDF3.columns)

  }

  def main(args: Array[String]): Unit = {
    demoStandardScaler()
  }

}
