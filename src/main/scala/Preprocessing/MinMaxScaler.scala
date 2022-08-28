package Preprocessing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, min}

import scala.annotation.tailrec

/**
 * @see https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html#sklearn.preprocessing.MinMaxScaler
 * @param inputDF
 * @param featureCols
 * @param featureRange
 */
class MinMaxScaler(inputDF: DataFrame, featureCols: List[String],
                   featureRange: Option[(Double, Double)] = Option(0.0, 1.0)) {

  def fitTransform(): DataFrame = {

    if (featureRange.isDefined) {
      val minMaxCalc: (DataFrame, String) => DataFrame = (x, y) =>
        x.withColumn("minmax_" + y, (col(y) - featureRange.get._1) / (featureRange.get._2 - featureRange.get._1))

      def minMaxCalcFunc(df: DataFrame): DataFrame = {
        val featureColsIter = featureCols.iterator
        @tailrec
        def recursorHelper(acc: DataFrame): DataFrame = {
          if (!featureColsIter.hasNext) acc
          else recursorHelper(minMaxCalc(acc, featureColsIter.next()))
        }
        recursorHelper(df)
      }

      val fitTransformCalc: (DataFrame, String) => DataFrame = (x, y) =>
        x.withColumn("std_" + y, col(y) * (featureRange.get._2 - featureRange.get._1) + featureRange.get._1)

      def fitTransform(df: DataFrame): DataFrame = {
        val featureColsIter = featureCols.iterator
        @tailrec
        def recursorHelper(acc: DataFrame): DataFrame = {
          if (!featureColsIter.hasNext) acc
          else recursorHelper(fitTransformCalc(acc, featureColsIter.next()))
        }
        recursorHelper(df)
      }

      val minMaxDF: DataFrame = minMaxCalcFunc(inputDF)
      val fitTransformDF: DataFrame = fitTransform(minMaxDF)
      val finalDF: DataFrame = fitTransformDF.drop(featureCols.map("minmax_" + _): _*)
      finalDF
    } else {
      val standardizeFunc: (DataFrame, String) => DataFrame = (x, y) =>
        x.withColumn("std_" + y, (col(y) - x.select(min(y)).head().getDouble(0)) /
          (x.select(max(y)).head().getDouble(0) - x.select(min(y)).head().getDouble(0)))

      def fitTransformFunc(df: DataFrame): DataFrame = {
        val featureColsIter = featureCols.iterator
        @tailrec
        def recursorHelper(acc: DataFrame): DataFrame = {
          if (!featureColsIter.hasNext) acc
          else recursorHelper(standardizeFunc(acc, featureColsIter.next()))
        }
        recursorHelper(df)
      }

      fitTransformFunc(inputDF)
    }
  }

}

