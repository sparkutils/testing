package com.sparkutils.testing

import com.sparkutils.testing.TestUtilsEnvironment.{onDatabricksFS, onFabricOrSynapse}
import org.apache.spark.sql.SparkSession

case class Sessions(classic: Option[SparkSession], connect: Option[ConnectSession]) {
  protected[testing] def stop(runWith: ConnectionType): Unit =
    if (shouldStop) {
      runWith match {
        case UseBoth =>
          classic.foreach(_.stop())
          connect.foreach(_.sparkSession.stop())
        case ClassicOnly =>
          classic.foreach(_.stop())
        case ConnectOnly =>
          connect.foreach(_.sparkSession.stop())
      }
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
 *
 * NOTE Spark does not allow more than one Classic SparkSession per VM (i.e. local master driver).
 *
 */
trait SessionStrategy extends ConnectUtils with ClassicUtils {

  /**
   * Override to set a single type of connection.  The default of UseBoth and the default createSparkSessions function
   * allows the system variable SPARKUTILS_DISABLE_CONNECT_TESTS to disable connect usage.
   * @return
   */
  val connectionType: ConnectionType = UseBoth

  /**
   * Suite wide default for which sessions to use for test runs.  An error will be raised if the SessionStrategy does not match
   * @return
   */
  val runWith: ConnectionType = UseBoth

  protected[testing] def verifyRunWith(): Unit = (runWith, connectionType) match {
    case (ConnectOnly, ConnectOnly | UseBoth) => ()
    case (ClassicOnly, ClassicOnly | UseBoth) => ()
    case (UseBoth, UseBoth) => ()
    case _ => sys.error(s"Testing runWith and connectionType must be appropriate combinations, instead got $runWith and $connectionType")
  }

  /**
   * Utility function to create sessions, delegates to classicSparkSession and connectSparkSession
   * @return
   */
  protected def createSparkSessions(connectionType: ConnectionType): Sessions = {
    val sessions =
      connectionType match {
        case ClassicOnly => Sessions(classicSparkSession, None)
        case ConnectOnly => Sessions(None, connectSparkSession)
        case UseBoth => Sessions(classicSparkSession, connectSparkSession)
      }
    setupSessions(sessions)
    sessions
  }

  /**
   * Called when a new session is created.
   *
   * NOTE no functions, such as withClassicAsActive, forEachSession etc. can
   * be used, a SOE awaits unless their overloaded version with a sessions param is used.
   * The same is true for classicOnly/connectOnly style functions that call the sessions function.
   */
  def setupSessions(sessions: Sessions): Unit = {

  }


  def sessions: Sessions

  /**
   * forces a stop on the sessions, then re-recreates the sessions.
   * This is required as there can only be one spark session per jvm.
   *
   * By default, this simply calls thunk
   */
  def swapSession[T](thunk: => T): T = thunk

}

/**
 * Utility interface to maintain session state
 */
trait SessionsStateHolder {
  def setSessions(sessions: => Sessions): Unit
  def getSessions: Sessions

  protected def stopConnectServer: Boolean = true

  /**
   * forces a stop on the sessions, then re-recreates the sessions after calling the thunk.
   * This is required as there can only be one spark session per jvm.
   */
  def swapSession[T](runWith: ConnectionType, create: => Sessions, thunk: => T): T = {
    try {
      stop(runWith)
      thunk
    } finally {
      setSessions(create)
    }
  }

  /**
   * By default, closes all client sessions and connect servers
   */
  def stop(runWith: ConnectionType = UseBoth): Unit =
    if (getSessions ne null) {
      val sessions = getSessions
      sessions.stop(runWith)

      if (stopConnectServer) {
        sessions.connect.foreach(_.stopServer())
      }
    }
}
