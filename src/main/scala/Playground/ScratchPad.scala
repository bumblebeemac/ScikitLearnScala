package Playground

import org.apache.spark.sql.SparkSession

object ScratchPad extends App {

//  val spark: SparkSession = SparkSession.builder()
//    .appName("PreprocessingStandardScaler")
//    .master("local[*]")
//    .getOrCreate()

  val aArray: Array[String] = Array("A", "B", "C", "D", "E")
  val removeElems: Array[String] = Array("C", "D")
  val leftOverElems: Array[String] = aArray diff removeElems
  println(leftOverElems.mkString(","))

}
