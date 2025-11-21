package com.sparkutils.testing

import com.sparkutils.testing.Utils.booleanEnvOrProp
import org.apache.spark.sql.SparkConnectServerUtils
import org.apache.spark.sql.SparkSession

trait ConnectUtils extends SparkConnectConfig {

  val disableConnectTesting = booleanEnvOrProp("SPARKUTILS_DISABLE_CONNECT_TESTS")

  def sparkClassicConfig(): Map[String, String]

  def connectSparkSession: Option[ConnectSession] =
    if (!disableConnectTesting) {
      SparkConnectServerUtils.localConnectServerForTesting(sparkClassicConfig() ++ sparkConnectServerConfig(), sparkConnectClientConfig())
    } else
      None
}

trait ConnectSession {
  /**
   *
   * @return The current SparkSession
   */
  def sparkSession: SparkSession

  /**
   * creates a new SparkSession, the pre-existing session will be stopped.
   */
  def resetSession(): Unit

  def stopServer(): Unit
}