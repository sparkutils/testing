package com.sparkutils.testing

trait SparkClassicConfig {
  /**
   * Builds a spark-defaults conf for use on the spark classic instance
   */
  def sparkClassicConfig(): Map[String, String]
}
