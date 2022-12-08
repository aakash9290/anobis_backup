package com.swiggy

import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDateTime}

object SimpleApplication extends App{
  val sparkConfigUtil = SparkConfigUtil(args)
  val logFile = "README.md" // Should be some file on your system

  val spark = if("PRODUCTION".equalsIgnoreCase(sparkConfigUtil.getDeploymentEnvironment.get.get)){
    SparkSession.builder.appName("Simple Application").getOrCreate()
  }else{
    SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
  }

  val database = "swiggy_order_management"
  val table = "swiggy_orders"
  val user = sparkConfigUtil.getUserName.get.get
  val password = sparkConfigUtil.getPassword.get.get
  val portNumber = "3306"
  val connString = "jdbc:mysql://localhost:" + portNumber + "/" + database

  val currentTime: LocalDateTime = LocalDateTime.now(Clock.systemUTC())
  val fromTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(currentTime.minusMinutes(30))
  val toTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(currentTime)

  println(s"SELECT * FROM ${table} WHERE created_on >= '${fromTime}' and created_on < '${toTime}'")

  val jdbcDF = spark.read.format("jdbc")
    .option("url", connString)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    //Use filter by specifying filter in dbtable option
    .option("dbtable", s"(SELECT * FROM ${table} WHERE created_on >= '${fromTime}' and created_on < '${toTime}') as swiggy_orders_t")
    .option("partitionColumn", "created_on")
    .option("lowerBound", fromTime)
    .option("upperBound", toTime)
    .option("numPartitions", "1")
    .load()

  //jdbcDF.show()
}