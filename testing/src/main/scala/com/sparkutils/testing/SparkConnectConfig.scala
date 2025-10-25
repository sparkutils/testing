package com.sparkutils.testing

trait SparkConnectConfig {

  /**
   * Builds a spark-defaults conf for use on the spawned connect.  The value is expected to be constant
   * Utils.mainClassPathsConfig can be used to pass in additional server classpath configuration
   */
  def sparkConnectServerConfig(): Map[String, String]

  /**
   * Configuration for the client builder
   */
  def sparkConnectConfig(): Map[String, String] = Map.empty
}
