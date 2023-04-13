ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.10"
ThisBuild / name := "scala-spark-3"
ThisBuild / organization := "rizwan.minhas"

lazy val root = (project in file(".")).settings(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % Constants.sparkVersion,
    "org.apache.spark" %% "spark-sql" % Constants.sparkVersion,
    "org.scalatest" %% "scalatest" % "3.2.15" % Test
  )
)