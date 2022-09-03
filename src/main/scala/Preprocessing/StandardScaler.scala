package Preprocessing

import org.apache.spark.sql.functions.{col, lit, mean, stddev}
import org.apache.spark.sql.DataFrame

import scala.annotation.tailrec

/**
 * @see https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html#sklearn.preprocessing.StandardScaler
 * @param inputDF spark dataframe with feature columns to be scaled
 * @param featureCols list of string with feature column names
 */
class StandardScaler(inputDF: DataFrame, featureCols: List[String]) {

  def fit(): Map[String, Array[Double]] = {
    val meanArr: Array[Double] =
      featureCols.foldLeft(Array[Double]())((arr, col) => arr :+ inputDF.select(mean(col)).head().getDouble(0))

    val stddevArr: Array[Double] =
      featureCols.foldLeft(Array[Double]())((arr, col) => arr :+ inputDF.select(stddev(col)).head().getDouble(0))

    Map("Mean" -> meanArr, "Stddev" -> stddevArr)
  }

  def transform(): DataFrame = {

    val featureDF: DataFrame = inputDF.select(featureCols.map(col): _*)

    val standardizeFunc: (DataFrame, String) => DataFrame = (df, colName) => {
      val meanValue = df.select(mean(colName)).head().getDouble(0)
      val stddevValue = df.select(stddev(colName)).head().getDouble(0)
      df.withColumn(colName, (col(colName) - lit(meanValue)) / lit(stddevValue))
    }

    def transformFunc(df: DataFrame): DataFrame = {
      val featureColsIter = featureCols.iterator
      @tailrec
      def recursorHelper(acc: DataFrame): DataFrame = {
        if (!featureColsIter.hasNext) acc
        else recursorHelper(standardizeFunc(acc, featureColsIter.next()))
      }

      recursorHelper(df)
    }

    val transformedDF: DataFrame = transformFunc(featureDF)
    transformedDF
  }

}
