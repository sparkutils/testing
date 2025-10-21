package com.sparkutils.testing.sessionStrategies

import com.sparkutils.testing.{SessionStrategy, Sessions, SessionsStateHolder}
import org.scalatest.{BeforeAndAfterAll, TestSuite}

/**
 * Creates a new set of sessions for every test suite but does not do so when a cluster is detected.
 * Each suite also starts new background Connect servers
 */
trait NewSessionEverySuite extends NewSessionEverySuiteBase { self: TestSuite =>

  val ssh =
    new SessionsStateHolder {
      var cur: Sessions = _
      override def setSessions(sessions: => Sessions): Unit = cur = sessions

      override def getSessions: Sessions = cur
    }

  protected def currentSessionsHolder: SessionsStateHolder = ssh

}
