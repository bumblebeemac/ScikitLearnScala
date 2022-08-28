name := "ScikitLearnScala"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.2.1"
val slf4jVersion = "1.7.36"
val scalaLoggingVersion = "3.9.5"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
)
