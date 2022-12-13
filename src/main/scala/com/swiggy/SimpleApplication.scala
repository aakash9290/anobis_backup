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
  * Arguments Local localhost 3306 swiggy_order_management swiggy_orders s3://cdc-prod-data/anobis_backup/mysql/checkout/swiggy_order_management/swiggy_orders/sync
  *
  * Arguments Local localhost 3306 baseoms order_job s3://cdc-prod-data/anobis_backup/mysql/dashbaseoms/baseoms/instamart/sync/
  */

object SimpleApplication extends App {
  for(arg <- args){println(arg)}
  println(args.size)

  val deploymentEnv = args(0)
  val rdsHost = args(1)
  val portNumber = args(2)
  val databaseName = args(3)
  val tableName = args(4)
  val targetPath = args(5)
  val duration = if(args.length>=7){args(6).toLong}else{60}

  var partitionKey = "created_at"
  val connString = s"jdbc:mysql://${rdsHost}:${portNumber}/${databaseName}"
  val currentTime: LocalDateTime = LocalDateTime.now(Clock.systemUTC())

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
      .option("numPartitions", "1")
      .load()
    FoodTransformation.process(jdbcFoodDF, 0)(spark, targetPath)
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
      .option("numPartitions", "1")
      .load()
    InstamartTransformation.process(jdbcInstamartDF, 0)(spark, targetPath)
  }
}
