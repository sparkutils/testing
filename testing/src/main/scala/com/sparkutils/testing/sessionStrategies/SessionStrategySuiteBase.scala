package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{ClassicOnly, ConnectOnly, SessionStrategy, Sessions, SessionsStateHolder, UseBoth}
import org.scalatest.{BeforeAndAfterAll, TestSuite}

/**
 * Creates a new set of sessions for every test suite but does not do so when a cluster is detected.
 * Implementations can override currentSessionsHolder to choose how to manage the sessions, or the sessions
 * method to change the creation approach
 */
trait SessionStrategySuiteBase extends SessionStrategy with BeforeAndAfterAll { self: TestSuite =>

  protected def currentSessionsHolder: SessionsStateHolder

  override def beforeAll(): Unit = {
    super.beforeAll()
    sessions
  }

  protected def shouldCallSessionsStop: Boolean = true

  override def afterAll(): Unit = {
    if (shouldCallSessionsStop) {
      currentSessionsHolder.stop(connectionType)
    }

    super.afterAll()
  }

  override def sessions: Sessions = {
    currentSessionsHolder.setSessions( createSparkSessions(connectionType) )
    currentSessionsHolder.getSessions
  }

  /**
   * The connect server is not restarted, but efforts are taken to not recreate unless runWith specifies it
   * @param thunk
   * @tparam T
   * @return
   */
  override def swapSession[T](thunk: => T): T = runWith match {
    case ClassicOnly =>
      // leave the connect stack alone
      val current = currentSessionsHolder.getSessions
      currentSessionsHolder.swapSession(runWith, {
        val classic = createSparkSessions(ClassicOnly)
        current.copy(classic = classic.classic)
      }, thunk)
    case UseBoth =>
      // keep the connect server around
      val current = currentSessionsHolder.getSessions
      currentSessionsHolder.swapSession(runWith, {
        val classic = createSparkSessions(ClassicOnly)
        current.connect.foreach(_.resetSession())
        current.copy(classic = classic.classic)
      }, thunk)
    case ConnectOnly =>
      // keep the connect server around
      val current = currentSessionsHolder.getSessions
      currentSessionsHolder.swapSession(runWith, {
        current.connect.foreach(_.resetSession())
        current
      }, thunk)
  }
}
