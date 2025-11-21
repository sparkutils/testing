package com.sparkutils.testing

import com.sparkutils.testing.SparkTestUtils.{booleanEnvOrProp, runtimeConnectClientConfig}
import org.apache.spark.sql.SparkConnectServerUtils
import org.apache.spark.sql.SparkSession

trait ConnectUtils extends SparkConnectConfig {

  val disableConnectTesting = booleanEnvOrProp("SPARKUTILS_DISABLE_CONNECT_TESTS")

  def sparkClassicConfig(): Map[String, String]

  def connectSparkSession: Option[ConnectSession] =
    if (!disableConnectTesting) {
      SparkConnectServerUtils.localConnectServerForTesting(sparkClassicConfig() ++
        sparkConnectServerConfig(), sparkConnectClientConfig()).map{
        s =>
          // proxy the underlying connection to set runtime config
          new ConnectSession {
            /**
             *
             * @return The current SparkSession
             */
            override def sparkSession: SparkSession = {
              val spark = s.sparkSession
              runtimeConnectClientConfig.foreach(p => spark.conf.set(p._1, p._2))
              spark
            }

            /**
             * creates a new SparkSession, the pre-existing session will be stopped.
             */
            override def resetSession(): Unit = s.resetSession()

            override def stopServer(): Unit = s.stopServer()
          }
      }
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