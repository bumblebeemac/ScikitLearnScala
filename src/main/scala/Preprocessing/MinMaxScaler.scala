package Preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, max, min}

import scala.annotation.tailrec

/**
 * @see https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html#sklearn.preprocessing.MinMaxScaler
 * @param inputDF spark dataframe with feature columns to be scaled
 * @param featureCols list of string with feature column names
 * @param featureRange option tuple with minimum and maximum of feature range
 */
class MinMaxScaler(inputDF: DataFrame, featureCols: List[String],
                   featureRange: Option[(Double, Double)] = Option((0.0, 1.0))) {

  val featureMin: Double = featureRange.get._1
  val featureMax: Double = featureRange.get._2

  def fitTransform(): DataFrame = {
    val featureDF = inputDF.select(featureCols.map(col): _*)

    val standardizer: (DataFrame, String) => DataFrame = (df, colName) => {
      val minValue: Double = df.agg(min(colName)).head().getString(0).toDouble
      val maxValue: Double = df.agg(max(colName)).head().getString(0).toDouble
      df.withColumn(colName, (col(colName) - lit(minValue)) / (lit(maxValue) - lit(minValue)))
    }

    def standardizedFunc(df: DataFrame): DataFrame = {
      val iterCols = featureCols.iterator
      @tailrec
      def recursorHelper(acc: DataFrame): DataFrame = {
        if (!iterCols.hasNext) acc
        else recursorHelper(standardizer(acc, iterCols.next()))
      }
      recursorHelper(df)
    }

    val scaler: (DataFrame, String) => DataFrame = (x, y) =>
      x.withColumn(y, col(y) * (lit(featureMax) - lit(featureMin)) + lit(featureMin))

    def scaleFunc(df: DataFrame): DataFrame = {
      val iterCols = featureCols.iterator
      @tailrec
      def recursorHelper(acc: DataFrame): DataFrame = {
        if (!iterCols.hasNext) acc
        else recursorHelper(scaler(acc, iterCols.next()))
      }
      recursorHelper(df)
    }

    val resultDF: DataFrame =
      if (featureMin == 0.0 && featureMax == 1.0) standardizedFunc(featureDF)
      else scaleFunc(standardizedFunc(featureDF))

    resultDF
  }

}

