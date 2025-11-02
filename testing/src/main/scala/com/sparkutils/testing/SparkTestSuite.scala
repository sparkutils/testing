package com.sparkutils.testing

import com.sparkutils.testing.SparkTestWrapper.{callingTestInClassic, callingTestInConnect}
import org.apache.spark.sql.SparkSession
import org.scalatest.{Outcome, TestSuite}

/**
 * Runs tests against both Spark Connect and Spark Classic.  Use the NoSparkConnect tag to disable connect usage for tests.
 */
trait SparkTestSuite extends TestUtils with TestSuite with ShouldRunWithoutSparkConnect { self: SessionStrategy =>

  type TestType = NoArgTest

  override def disableSparkConnect(test: NoArgTest): Boolean =
    test.tags.contains(NoSparkConnect.name)

  override def withFixture(test: NoArgTest): org.scalatest.Outcome = {
    sessions // called for side effect so logging works

    SparkTestWrapper.wrap(super.withFixture)(_.isSucceeded)(test,
      this: TestUtils with SessionStrategy with ShouldRunWithoutSparkConnect { type TestType = NoArgTest })()
  }

  /**
   * When a classic is available it sets it as the active session
   * @param f
   * @tparam T
   */
  def withClassicAsActive(f: => Unit): Unit = {
    var active: Option[SparkSession] = None
    try {
      active = SparkSession.getActiveSession

      val sess = sessions

      sess.classic.foreach { s =>
        inConnect.set(false)
        SparkSession.setActiveSession(s)

        f
      }
    } finally {
      active.foreach( s => SparkSession.setActiveSession(s) )
    }
  }

  /**
   * Calls this function for each session, resetting the active session to the previous version, ideal for teardown functions
   * @param f
   */
  def forEachSession( f: SparkSession => Unit): Unit = {
    // simple visual markers in stack trace, can evolve better errors later
    def callingInClassic(s: SparkSession): Unit = f(s)
    def callingInConnect(s: SparkSession): Unit = f(s)

    val sess = sessions

    var active: Option[SparkSession] = None
    try {
      active = SparkSession.getActiveSession
      sess.classic.foreach { s =>
        inConnect.set(false)
        SparkSession.setActiveSession(s)

        callingInClassic(s)
      }

      sess.connect.foreach { s =>
        inConnect.set(true)
        SparkSession.setActiveSession(s.sparkSession)

        callingInConnect(s.sparkSession)
      }
    } finally {
      active.foreach( s => SparkSession.setActiveSession(s) )
    }
  }
}

object SparkTestWrapper {

  // simple visual markers in stack trace, can evolve better errors later
  private def callingTestInClassic[T,R](testFunction: T => R)(t: T): R = testFunction(t)
  private def callingTestInConnect[T,R](testFunction: T => R)(t: T): R = testFunction(t)

  def wrap[T,R](testFunction: T => R)(isSucceeded: R => Boolean)(test: T, testUtils: TestUtils with SessionStrategy with ShouldRunWithoutSparkConnect { type TestType = T })(printF: String => Unit = print): R = {
    import testUtils._

    cleanupOutput()

    val sess = sessions

    val classic = sess.classic.map{ s =>
      inConnect.set(false)
      SparkSession.setActiveSession(s)

      callingTestInClassic(testFunction)(test)
    }

    if (classic.isEmpty || (isSucceeded(classic.get) && sess.connect.isDefined && !disableSparkConnect(test))) {
      inConnect.set(true)
      SparkSession.setActiveSession(sess.connect.get.sparkSession)

      callingTestInConnect(testFunction)(test)
    } else
      classic.get
  }
}
