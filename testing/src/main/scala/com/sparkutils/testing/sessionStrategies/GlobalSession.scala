package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{Sessions, SessionsStateHolder}

/**
 * Convenience singleton to store a single Sessions object for the test run.
 *
 * The SessionStrategy defines how this is used.
 *
 * Note that closing the spark sessions directly results in undefined behaviour
 */
object GlobalSession extends SessionsStateHolder {

  private var current: Sessions = _

  override def setSessions(sessions: => Sessions): Unit = {
    current = sessions
  }

  override def getSessions: Sessions = current

  override def stop(): Unit = {
    super.stop()
    current = null
  }
}