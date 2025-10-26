package org.apache.spark.sql

import com.sparkutils.testing.ConnectSession

object SparkConnectServerUtils {
  def localConnectServerForTesting(serverConfig: Map[String, String], clientConfig: Map[String, String]): Option[ConnectSession] = None
}