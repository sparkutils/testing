package com.sparkutils.testing

trait SparkClassicConfig {
  /**
   * Builds a spark-defaults conf for use on the spark classic instance.
   *
   * By default it includes testing-runtimes Utils.additionalClassicConfig, be sure to call super's when overriding.
   */
  def sparkClassicConfig(): Map[String, String]
}
