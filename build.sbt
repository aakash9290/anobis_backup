ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

mainClass := Some("com/swiggy/ingestion/spark_executor/common/SparkExecutor.scala")
crossPaths := false

lazy val root = (project in file("."))
  .settings(
    name := "anobis_backup"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"
