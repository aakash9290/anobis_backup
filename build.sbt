import sbt.Keys.libraryDependencies
import sbt.ThisBuild

mainClass := Some("com/swiggy/SimpleApplication")
crossPaths := false

lazy val commonSettings = Seq(
  ThisBuild / scalaVersion := "2.12.8",
  ThisBuild / version := "0.1.0-SNAPSHOT",
  ThisBuild / organization := "com.swiggy",
  ThisBuild / organizationName := "anobis-backup",
) ++ Seq(assemblyMerge)

lazy val root = (project in file("."))
  .settings(commonSettings:_*)
  .enablePlugins(AssemblyPlugin)

val assemblyMerge = assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"
libraryDependencies += "io.delta" %% "delta-core" % "1.1.0"
// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.10.0-RC7"
