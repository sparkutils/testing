package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{ConnectionType, SessionStrategy, Sessions, SessionsStateHolder}
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
      currentSessionsHolder.stop()
    }

    super.afterAll()
  }

  override def sessions: Sessions = {
    currentSessionsHolder.setSessions( createSparkSessions(connectionType) )
    currentSessionsHolder.getSessions
  }
}
