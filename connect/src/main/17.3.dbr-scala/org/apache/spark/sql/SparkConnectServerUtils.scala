package org.apache.spark.sql

import com.sparkutils.testing.ConnectSession
import org.apache.spark.sql
import org.apache.spark.sql.connect.SparkSession

object SparkConnectServerUtils {
  /**
   * Note, although 17.3 allows spark.api.mode you still need to register any sparksessions at the cluster level,
   * config to use the sessions in your code is not enough
   * @return
   */
  def localConnectServerForTesting(serverConfig: Map[String, String], clientConfig: Map[String, String]): Option[ConnectSession] =
    Some(new ConnectSession {

      private def createSparkSession: SparkSession =
        SparkSession.getActiveSession.getOrElse {
          // check getOrElse to stop warn:
          // 25/12/05 11:56:55 WARN SparkSession: spark.api.mode configuration is not supported in Connect mode.
          SparkSession.builder.
            config(clientConfig).config("spark.api.mode", "connect").getOrCreate()
        }

      private var _sparkSession: SparkSession = createSparkSession

      override def sparkSession: sql.SparkSession = _sparkSession

      def stopServer(): Unit = {}

      override def resetSession(): Unit = {}
    })

}