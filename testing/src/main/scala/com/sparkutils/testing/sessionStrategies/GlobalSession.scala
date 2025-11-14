package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{ConnectionType, Sessions, SessionsStateHolder}

/**
 * Convenience singleton to store a single Sessions object for the test run.  Any ConnectServers created will
 * not be stopped when calling stop
 *
 * The SessionStrategy defines how this is used.
 *
 * Note that closing the spark sessions directly results in undefined behaviour
 */
object GlobalSession extends SessionsStateHolder {
  override protected def stopConnectServer: Boolean = false

  private var current: Sessions = _

  override def setSessions(sessions: => Sessions): Unit = {
    current = sessions
  }

  override def getSessions: Sessions = current

  override def stop(runWith: ConnectionType): Unit = {
    super.stop(runWith)
    current = null
  }
}