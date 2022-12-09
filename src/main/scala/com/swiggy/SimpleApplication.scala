package com.swiggy

import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDateTime}
import scala.util.Try
import play.api.libs.json._
import scala.util.{Failure, Success}
import org.apache.spark.sql.functions._


/**
 * This is main class
 * Arguments --deploymentEnvironment Local --rdsHost localhost --portNumber 3306 --userName root --password root --databaseName swiggy_order_management --tableName swiggy_orders --targetPath s3://cdc-prod-data/anobis_backup/mysql/checkout/swiggy_order_management/swiggy_orders/sync
 *
 * Arguments --deploymentEnvironment Local --rdsHost localhost --portNumber 3306 --userName root --password root --databaseName baseoms --tableName order_job --targetPath s3://cdc-prod-data/anobis_backup/mysql/dashbaseoms/baseoms/instamart/sync/
 */

object SimpleApplication extends App{
  val sparkConfigUtil = SparkConfigUtil(args)

  val deploymentEnv = sparkConfigUtil.getDeploymentEnvironment.get.get
  val rdsHost = sparkConfigUtil.getRdsHost.get.get
  val portNumber = sparkConfigUtil.getPortNumber.get.get
  val user = sparkConfigUtil.getUserName.get.get
  val password = sparkConfigUtil.getPassword.get.get
  val databaseName = sparkConfigUtil.getDatabaseName.get.get
  val tableName = sparkConfigUtil.getTableName.get.get
  val targetPath = sparkConfigUtil.getTargetPath.get.get

  var partitionKey = "created_at"
  val connString = s"jdbc:mysql://${rdsHost}:${portNumber}/${databaseName}"
  val currentTime: LocalDateTime = LocalDateTime.now(Clock.systemUTC())
  val fromTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(currentTime.minusMinutes(60))
  val toTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(currentTime)

  println("connString -->"+connString)

  val spark = if("PRODUCTION".equalsIgnoreCase(deploymentEnv)){
    SparkSession.builder.appName("Simple Application").config("spark.databricks.delta.schema.autoMerge.enabled", true).getOrCreate()
  }else{
    SparkSession.builder.appName("Simple Application").config("spark.databricks.delta.schema.autoMerge.enabled", true).master("local[*]").getOrCreate()
  }

  if("swiggy_orders".equalsIgnoreCase(tableName)) {
   partitionKey = "created_on"
  }


  val jdbcDF = spark.read.format("jdbc")
    .option("url", connString)
    .option("dbtable", tableName)
    .option("user", user)
    .option("password", password)
    //Use filter by specifying filter in dbtable option
    .option(s"dbtable", s"(SELECT * FROM ${tableName} WHERE ${partitionKey} >= '${fromTime}' and ${partitionKey} < '${toTime}') as src_db")
    .option("partitionColumn", s"${partitionKey}")
    .option("lowerBound", fromTime)
    .option("upperBound", toTime)
    .option("numPartitions", "1")
    .load()

  jdbcDF.show()

  if("swiggy_orders".equalsIgnoreCase(tableName)) {
    import org.apache.spark.sql.streaming.{OutputMode, Trigger}
    FoodTransformation.process(jdbcDF, 0)(spark)
  } else {
    InstamartTransformation.process(jdbcDF, 0)(spark)
  }
}