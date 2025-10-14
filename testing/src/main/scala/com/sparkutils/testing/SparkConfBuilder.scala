package com.sparkutils.testing

trait SparkConfBuilder {
  /**
   * Builds a spark-defaults conf for use on the spawned connect
   */
  def buildSparkConf(): Unit
}
