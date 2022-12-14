package com.swiggy

import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDateTime}
import java.util.Base64 // for base64
import java.nio.charset.StandardCharsets // for base64
import scala.util.Try
import play.api.libs.json._
import scala.util.{Failure, Success}
import org.apache.spark.sql.functions._

/** This is main class
  * Arguments --deploymentEnvironment Local --rdsHost localhost --portNumber 3306 --userName root --password root --databaseName swiggy_order_management --tableName swiggy_orders --targetPath s3://cdc-prod-data/anobis_backup/mysql/checkout/swiggy_order_management/swiggy_orders/sync
  *
  * Arguments --deploymentEnvironment Local --rdsHost localhost --portNumber 3306 --userName root --password root --databaseName baseoms --tableName order_job --targetPath s3://cdc-prod-data/anobis_backup/mysql/dashbaseoms/baseoms/instamart/sync/
  */

object SimpleApplication extends App {
  val sparkConfigUtil = SparkConfigUtil(args)

  val deploymentEnv = sparkConfigUtil.getDeploymentEnvironment.get.get
  val rdsHost = sparkConfigUtil.getRdsHost.get.get
  val portNumber = sparkConfigUtil.getPortNumber.get.get
  val databaseName = sparkConfigUtil.getDatabaseName.get.get
  val tableName = sparkConfigUtil.getTableName.get.get
  val targetPath = sparkConfigUtil.getTargetPath.get.get

  var partitionKey = "created_at"
  val numPartitions = sparkConfigUtil.getNumPartitions.get.getOrElse("1")
  val connString = s"jdbc:mysql://${rdsHost}:${portNumber}/${databaseName}"
  val currentTime: LocalDateTime = LocalDateTime.now(Clock.systemUTC())

  val duration = sparkConfigUtil.getDuration.get.getOrElse(60)
  val fromTime = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss")
    .format(currentTime.minusMinutes(duration))
  val toTime =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(currentTime)

  println("connString -->" + connString)

  val spark = if ("PRODUCTION".equalsIgnoreCase(deploymentEnv)) {
    SparkSession.builder
      .appName("Simple Application")
      .config("spark.databricks.delta.schema.autoMerge.enabled", true)
      .getOrCreate()
  } else {
    SparkSession.builder
      .appName("Simple Application")
      .config("spark.databricks.delta.schema.autoMerge.enabled", true)
      .master("local[*]")
      .getOrCreate()
  }

  spark.sparkContext.setLogLevel("ERROR")

  val uname_f = new String(
    Base64.getDecoder().decode(spark.conf.get("spark.jdbc.uname_f")),
    StandardCharsets.UTF_8
  )
  val ukey_f = new String(
    Base64.getDecoder().decode(spark.conf.get("spark.jdbc.ukey_f")),
    StandardCharsets.UTF_8
  )
  val uname_i = new String(
    Base64.getDecoder().decode(spark.conf.get("spark.jdbc.uname_i")),
    StandardCharsets.UTF_8
  )
  val ukey_i = new String(
    Base64.getDecoder().decode(spark.conf.get("spark.jdbc.ukey_i")),
    StandardCharsets.UTF_8
  )

  if ("swiggy_orders".equalsIgnoreCase(tableName)) {
    partitionKey = "created_on"
    val jdbcFoodDF = spark.read
      .format("jdbc")
      .option("url", connString)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", uname_f)
      .option("password", ukey_f)
      .option(
        s"dbtable",
        s"(SELECT * FROM ${tableName} WHERE ${partitionKey} >= '${fromTime}' and ${partitionKey} < '${toTime}') as src_db"
      )
      .option("partitionColumn", s"${partitionKey}")
      .option("lowerBound", fromTime)
      .option("upperBound", toTime)
      .option("numPartitions", numPartitions)
      .load()
    FoodTransformation.process(jdbcFoodDF, 0)(spark)
  } else {
    val jdbcInstamartDF = spark.read
      .format("jdbc")
      .option("url", connString)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", uname_i)
      .option("password", ukey_i)
      .option(
        s"dbtable",
        s"(SELECT * FROM ${tableName} WHERE ${partitionKey} >= '${fromTime}' and ${partitionKey} < '${toTime}') as src_db"
      )
      .option("partitionColumn", s"${partitionKey}")
      .option("lowerBound", fromTime)
      .option("upperBound", toTime)
      .option("numPartitions", numPartitions)
      .load()
    InstamartTransformation.process(jdbcInstamartDF, 0)(spark)
  }
}
