package Preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, max}

import scala.annotation.tailrec

// TODO - NEED TO FIX LOGIC

/**
 * @see https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MaxAbsScaler.html#sklearn.preprocessing.MaxAbsScaler
 * @param inputDF spark dataframe with feature columns to be scaled
 * @param featureCols list of string with feature column names
 */
class MaxAbsScaler(inputDF: DataFrame, featureCols: List[String]) {

  def fitTransform(): DataFrame = {
    val featureDF = inputDF.select(featureCols.map(col): _*)

    val scaler: (DataFrame, String) => DataFrame = (df, colName) => {
      val maxValue = df.agg(max(colName)).head().getString(0).toDouble
      df.withColumn(colName, col(colName) / lit(maxValue))
    }

    def scalerFunc(df: DataFrame): DataFrame = {
      val iterCols = featureCols.iterator
      @tailrec
      def recursorHelper(acc: DataFrame): DataFrame = {
        if (!iterCols.hasNext) acc
        else recursorHelper(scaler(acc, iterCols.next()))
      }
      recursorHelper(df)
    }

    scalerFunc(featureDF)
  }

}
