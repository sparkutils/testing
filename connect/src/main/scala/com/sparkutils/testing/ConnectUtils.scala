package com.sparkutils.testing

import com.sparkutils.testing.Utils.booleanEnv
import org.apache.spark.sql.SparkConnectServerUtils
import org.apache.spark.sql.SparkSession

trait ConnectUtils extends SparkConnectConfig {

  val disableConnectTesting = booleanEnv("SPARKUTILS_DISABLE_CONNECT_TESTS")

  def sparkClassicConfig(): Map[String, String]

  def connectSparkSession: Option[ConnectSession] =
    if (!disableConnectTesting) {
      SparkConnectServerUtils.localConnectServerForTesting(sparkClassicConfig() ++ sparkConnectServerConfig(), sparkConnectClientConfig())
    } else
      None
}

trait ConnectSession {
  def sparkSession: SparkSession

  def stopServer(): Unit
}