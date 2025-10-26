package com.sparkutils.testing

trait SparkConnectConfig {

  /**
   * Override to control the connect server logging level - defaults to ERROR
   * @return
   */
  def connectServerLoggingLevel: String = "ERROR"

  /**
   * Builds a spark-defaults conf for use on the spawned connect.  The value is expected to be constant
   * Utils.mainClassPathsConfig can be used to pass in additional server classpath configuration
   */
  def sparkConnectServerConfig(): Map[String, String] = Map("spark.log.level" -> connectServerLoggingLevel)

  /**
   * Configuration for the client builder
   */
  def sparkConnectClientConfig(): Map[String, String] = Map.empty
}
