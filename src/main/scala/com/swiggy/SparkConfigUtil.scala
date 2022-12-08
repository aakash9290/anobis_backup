package com.swiggy

import org.rogach.scallop.{ScallopConf, _}
import org.slf4j.{Logger, LoggerFactory}

case class SparkConfigUtil(override val args: Seq[String])  extends ScallopConf(args) {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private val filePath: ScallopOption[String] = opt[String]("filePath")
  private val deploymentEnvironment: ScallopOption[String] =
    opt[String]("deploymentEnvironment")

  private val password: ScallopOption[String] =
    opt[String]("password")

  private val userName: ScallopOption[String] =
    opt[String]("userName")

  verify()

  def getDeploymentEnvironment = deploymentEnvironment
  def getUserName = userName
  def getPassword = password
}
