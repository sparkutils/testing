package com.sparkutils.testing

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

}

object SparkTestWrapper {

  // simple visual markers in stack trace, can evolve better errors later
  private def callingTestInClassic[T,R](testFunction: T => R)(t: T): R = testFunction(t)
  private def callingTestInConnect[T,R](testFunction: T => R)(t: T): R = testFunction(t)

  def wrap[T,R](testFunction: T => R)(isSucceeded: R => Boolean)(test: T, testUtils: TestUtils with SessionStrategy with ShouldRunWithoutSparkConnect { type TestType = T })(printF: String => Unit = print): R = {
    import testUtils._

    cleanupOutput()

    val classic = sessions.classic.map{ s =>
      inConnect.set(false)

      callingTestInClassic(testFunction)(test)
    }

    if (classic.isEmpty || (isSucceeded(classic.get) && sessions.connect.isDefined && !disableSparkConnect(test))) {
      inConnect.set(true)

      callingTestInConnect(testFunction)(test)
    } else
      classic.get
  }
}
