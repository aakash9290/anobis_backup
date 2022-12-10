package com.swiggy

import org.rogach.scallop.{ScallopConf, _}

case class SparkConfigUtil(override val args: Seq[String])  extends ScallopConf(args) {

  private val deploymentEnvironment: ScallopOption[String] =
    opt[String]("deploymentEnvironment")
  private val rdsHost: ScallopOption[String] =
    opt[String]("rdsHost")
  private val portNumber: ScallopOption[String] =
    opt[String]("portNumber")
  private val databaseName: ScallopOption[String] =
    opt[String]("databaseName")
  private val tableName: ScallopOption[String] =
    opt[String]("tableName")
  private val targetPath: ScallopOption[String] =
    opt[String]("targetPath")

  verify()

  def getDeploymentEnvironment = deploymentEnvironment
  def getRdsHost = rdsHost
  def getPortNumber = portNumber
  def getDatabaseName = databaseName
  def getTableName = tableName
  def getTargetPath = targetPath
}
