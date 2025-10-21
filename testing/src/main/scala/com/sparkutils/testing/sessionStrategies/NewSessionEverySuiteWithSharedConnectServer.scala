package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{ClassicOnly, Sessions, SessionsStateHolder}
import org.scalatest.TestSuite

/**
 * Creates a new set of sessions for every test suite but does not do so when a cluster is detected.
 * The currentSessionsHolder.setSessions function is called with a fresh Classic (if configured) but
 * with the Connect server shared across all invocations, the implementer should use a singleton stateholder but
 * must override stopConnectServer to return false.
 */
trait NewSessionEverySuiteWithSharedConnectServer extends NewSessionEverySuiteBase {
  self: TestSuite =>

  /**
   * only creates classic sessions after the first invocation
   * @return
   */
  override def sessions: Sessions = {
    val cur = currentSessionsHolder.getSessions
    currentSessionsHolder.setSessions(
      if (cur eq null)
        createSparkSessions(connectionType)
      else
        // re-use the current connect server, but create a fresh classic spark session
        cur.connect.map(cs => createSparkSessions(ClassicOnly).copy(connect = Some(cs))).getOrElse(
          createSparkSessions(connectionType)
        )
    )
    currentSessionsHolder.getSessions
  }
}
