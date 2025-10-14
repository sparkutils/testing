package com.sparkutils.testing

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.Try

trait ConnectUtils { this: SparkConfBuilder =>

  val disableConnectTesting = {
    val tmp = System.getenv("SPARKUTILS_DISABLE_CONNECT_TESTS")
    if (tmp eq null)
      false
    else
      Try{tmp.toBoolean}.getOrElse(false)
  }

  lazy val connectSparkSession: Option[SparkSession] = {
    if (!disableConnectTesting) {
      SparkTestUtils.localConnectServerForTesting
    } else
      None
  }
  lazy val connectSqlContext: Option[SQLContext] = connectSparkSession.map(_.sqlContext)

}
