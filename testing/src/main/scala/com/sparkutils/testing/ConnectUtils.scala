package com.sparkutils.testing

import org.apache.spark.sql.{SQLContext, SparkSession}

trait ConnectUtils {

  val disableConnectTesting = {
    val tmp = System.getenv("SPARKUTILS_DISABLE_CONNECT_TESTS")
    if (tmp eq null)
      null
    else
      tmp
  }

  lazy val connectSparkSession: Option[SparkSession] = {
    if (disableConnectTesting ne null && disableConnectTesting.toBoolean)
      SparkTestUtils.localConnectServerForTesting
    else
      None
  }
  lazy val connectSqlContext: Option[SQLContext] = connectSparkSession.map(_.sqlContext)

}
