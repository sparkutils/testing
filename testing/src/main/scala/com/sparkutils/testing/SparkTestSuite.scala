package com.sparkutils.testing

import com.sparkutils.testing.SparkTestUtils.disableClassicTesting
import org.apache.spark.sql.{ShimUtils, SparkSession}
import org.scalatest.TestSuite

/**
 * Runs tests against both Spark Connect and Spark Classic.  Use the NoSparkConnect tag to disable connect usage for specific tests or override runWith for the entire suite.
 */
trait SparkTestSuite extends TestUtils with TestSuite with ShouldRunWithoutSparkConnect { self: SessionStrategy =>

  type TestType = NoArgTest

  override def disableSparkConnect(test: NoArgTest): Boolean =
    test.tags.contains(NoSparkConnect.name)

  protected lazy val forceLoad: Sessions = sessions // called for side effect so logging works

  override def withFixture(test: NoArgTest): org.scalatest.Outcome = {
    forceLoad
    SparkTestWrapper.wrap(super.withFixture)(_.isSucceeded)(org.scalatest.Succeeded)(test,
      this: TestUtils with SessionStrategy with ShouldRunWithoutSparkConnect { type TestType = NoArgTest })()
  }

  /**
   * When a classic is available it sets it as the active session.
   *
   * NOTE Use the overloaded version of withClassicAsActive in setupSessions functions
   * @param f
   * @tparam T
   */
  def withClassicAsActive(f: => Unit): Unit = {
    var active: Option[SparkSession] = None
    try {
      active = SparkSession.getActiveSession

      val sess = sessions

      withClassicAsActive(sess, f)
    } finally {
      active.foreach( s => SparkSession.setActiveSession(s) )
    }
  }

  /**
   * When a classic is available it sets it as the active session
   * @param f
   * @tparam T
   */
  def withClassicAsActive(sessions: Sessions, f: => Unit): Unit = {
    if (sessions.classic.isEmpty && !disableClassicTesting) {
      sys.error("withClassicAsActive called but there is no classic session")
    }

    sessions.classic.foreach { s =>
      if (!ShimUtils.isUsable(s)) {
        sys.error("withClassicAsActive called but the session is stopped")
      }

      inConnect.set(false)
      SparkSession.setActiveSession(s)

      f
    }
  }

  /**
   * Calls this function for each session, resetting the active session to the previous version, ideal for teardown functions
   * @param f
   */
  def forEachSession(sessions: Sessions, f: SparkSession => Unit): Unit = {
    // simple visual markers in stack trace, can evolve better errors later
    def callingInClassic(s: SparkSession): Unit = f(s)
    def callingInConnect(s: SparkSession): Unit = f(s)

    sessions.classic.foreach { s =>
      inConnect.set(false)
      SparkSession.setActiveSession(s)

      callingInClassic(s)
    }

    sessions.connect.foreach { s =>
      inConnect.set(true)
      SparkSession.setActiveSession(s.sparkSession)

      callingInConnect(s.sparkSession)
    }
  }

  /**
   * Calls this function for each session, resetting the active session to the previous version, ideal for teardown functions
   *
   * NOTE Use the overloaded version of forEachSession in setupSessions functions
   * @param f
   */
  def forEachSession( f: SparkSession => Unit): Unit = {
    val sess = sessions

    var active: Option[SparkSession] = None
    try {
      active = SparkSession.getActiveSession
      forEachSession(sess, f)
    } finally {
      active.foreach( s => SparkSession.setActiveSession(s) )
    }
  }
}

object SparkTestWrapper {

  // simple visual markers in stack trace, can evolve better errors later
  private def callingTestInClassic[T,R](testFunction: T => R)(t: T): R = testFunction(t)
  private def callingTestInConnect[T,R](testFunction: T => R)(t: T): R = testFunction(t)

  def wrap[T,R](testFunction: T => R)(isSucceeded: R => Boolean)(skipped: R)(test: T, testUtils: TestUtils with SessionStrategy with ShouldRunWithoutSparkConnect { type TestType = T })(printF: String => Unit = print): R = {
    import testUtils._

    testUtils.verifyRunWith()

    cleanupOutput()

    val sess = sessions

    val classic = sess.classic.flatMap{ s =>
      testUtils.runWith match {
        case UseBoth | ClassicOnly =>
          inConnect.set(false)
          SparkSession.setActiveSession(s)

          Some( callingTestInClassic(testFunction)(test) )
        case _ => None
      }
    }

    if (classic.isEmpty || (isSucceeded(classic.get) && sess.connect.isDefined && !disableSparkConnect(test))) {
      testUtils.runWith match {
        case UseBoth | ConnectOnly =>
          inConnect.set(true)
          SparkSession.setActiveSession(sess.connect.get.sparkSession)

          callingTestInConnect(testFunction)(test)
        case _ if classic.isDefined => classic.get
        case _ if !disableClassicTesting => sys.error(s"Testing TestSuite has runWith ClassicOnly, but no classic session exists despite not being disabled")
        case _ => skipped
      }
    } else
      classic.get
  }
}
