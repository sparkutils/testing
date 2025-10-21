package com.sparkutils.testing

import com.sparkutils.testing.TestUtilsEnvironment.{onDatabricksFS, onFabricOrSynapse}
import org.apache.spark.sql.SparkSession

case class Sessions(classic: Option[SparkSession], connect: Option[ConnectSession]) {
  protected[testing] def stop: Unit =
    if (shouldStop) {
      classic.foreach(_.stop())
      connect.foreach(_.sparkSession.stop())
    }

  protected[testing] def shouldStop: Boolean =
    classic.forall(c => !(onFabricOrSynapse(c) || onDatabricksFS))
}

/**
 * Which type of connection should be used?
 */
sealed trait ConnectionType
case object ClassicOnly extends ConnectionType
case object ConnectOnly extends ConnectionType
case object UseBoth extends ConnectionType

/**
 * Defines what happens with session handling.
 */
trait SessionStrategy extends ConnectUtils with ClassicUtils {

  /**
   * Override to set a single type of connection.  The default of UseBoth and the default createSparkSessions function
   * allows the system variable SPARKUTILS_DISABLE_CONNECT_TESTS to disable connect usage.
   * @return
   */
  def connectionType: ConnectionType = UseBoth

  /**
   * Utility function to create sessions, delegates to classicSparkSession and connectSparkSession
   * @return
   */
  protected def createSparkSessions(connectionType: ConnectionType): Sessions =
    connectionType match {
      case ClassicOnly => Sessions(classicSparkSession, None)
      case ConnectOnly => Sessions(None, connectSparkSession)
      case UseBoth => Sessions(classicSparkSession, connectSparkSession)
    }

  def sessions: Sessions

}

/**
 * Utility interface to maintain session state
 */
trait SessionsStateHolder {
  def setSessions(sessions: => Sessions): Unit
  def getSessions: Sessions

  protected def stopConnectServer: Boolean = true

  /**
   * By default closes all client sessions and connect servers
   */
  def stop(): Unit =
    if (getSessions ne null) {
      val sessions = getSessions
      sessions.stop

      if (stopConnectServer) {
        sessions.connect.foreach(_.stopServer())
      }
    }
}
