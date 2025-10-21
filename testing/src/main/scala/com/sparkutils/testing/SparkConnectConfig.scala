package com.sparkutils.testing

trait SparkConnectConfig {

  /**
   * Builds a spark-defaults conf for use on the spawned connect.  Only the first call will be successful
   */
  def sparkConnectServerConfig(): Map[String, String]

  /**
   * Configuration for the client builder
   */
  def sparkConnectConfig(): Map[String, String] = Map.empty
}
