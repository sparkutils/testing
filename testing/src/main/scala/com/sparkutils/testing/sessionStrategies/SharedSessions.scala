package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{ClassicOnly, Sessions}
import org.scalatest.TestSuite

/**
 * Shares sessions across the currentSessionsHolder.  Sessions.stop is never called and sessions are
 * created once only.
 */
trait SharedSessions extends SessionStrategySuiteBase {
  self: TestSuite =>

  override protected def shouldCallSessionsStop: Boolean = false

  /**
   * only creates sessions once and will re-use if the holder already has a session
   * @return
   */
  override def sessions: Sessions = {
    val cur = currentSessionsHolder.getSessions
    if (cur eq null) {
      currentSessionsHolder.setSessions {
        createSparkSessions(connectionType)
      }
      currentSessionsHolder.getSessions
    } else
      cur
  }
}
