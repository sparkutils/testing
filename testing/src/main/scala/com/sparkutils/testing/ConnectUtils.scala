package com.sparkutils.testing

import com.sparkutils.testing.Utils.booleanEnv
import org.apache.spark.sql.SparkSession

trait ConnectUtils extends SparkConnectConfig with SparkClassicConfig {

  val disableConnectTesting = booleanEnv("SPARKUTILS_DISABLE_CONNECT_TESTS")

  def connectSparkSession: Option[ConnectSession] =
    if (!disableConnectTesting) {
      SparkTestUtils.localConnectServerForTesting(sparkClassicConfig() ++ sparkConnectConfig())
    } else
      None
}

trait ConnectSession {
  def sparkSession: SparkSession

  def stopServer(): Unit
}