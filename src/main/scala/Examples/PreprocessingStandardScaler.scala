package Examples

import Preprocessing.{MinMaxScaler, StandardScaler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{mean, min, stddev}

object PreprocessingStandardScaler {

  val spark: SparkSession = SparkSession.builder()
    .appName("PreprocessingStandardScaler")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // Set in order to make sure Exception does not occur due to string concatenation
  spark.conf.set("spark.debug.maxToStringFields", 1000)

  def demoStandardScaler(): Unit = {
    val aDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .csv("src/data/usedcars_dataset.csv")

    val featureCols = List("compression_ratio", "horsepower", "peak_rpm", "city_mpg", "highway_mpg", "price")

    // StandardScaler section
    val scaler = new StandardScaler(aDF, featureCols)
    val transformedDF = scaler.transform()
    transformedDF.printSchema()
    transformedDF.show()

    // Test for zero mean and unit variance
    println(transformedDF.select(mean("std_compression_ratio")).head().getDouble(0))
    println(transformedDF.select(stddev("std_compression_ratio")).head().getDouble(0))

//    // MinMaxScaler section
//    val scaler = new MinMaxScaler(aDF, featureCols)
//    val transformedDF = scaler.fitTransform()
//    transformedDF.printSchema()
//    transformedDF.show()
  }

  def main(args: Array[String]): Unit = {
    demoStandardScaler()
  }

}
