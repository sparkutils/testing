package com.sparkutils.testing

import org.scalatest.TestSuite

/**
 * Runs tests against both Spark Connect and Spark Classic.  Use the NoSparkConnect tag to disable connect usage for tests.
 */
trait SparkTestSuite extends TestUtils with TestSuite with ShouldRunWithoutSparkConnect { self: SessionStrategy =>

  type TestType = NoArgTest

  override def disableSparkConnect(test: NoArgTest): Boolean =
    test.tags.contains(NoSparkConnect.name)

  override def withFixture(test: NoArgTest): org.scalatest.Outcome =
    SparkTestWrapper.wrap(super.withFixture)(_.isSucceeded)(test, this)()

}

object SparkTestWrapper {
  def wrap[T,R](testFunction: T => R)(isSucceeded: R => Boolean)(test: T, testUtils: TestUtils with SessionStrategy with ShouldRunWithoutSparkConnect { type TestType = T })(printF: String => Unit = print): R = {
    import testUtils._

    cleanupOutput()

    val classic =
    {
      inConnect.set(false)
      if (connectSparkSession.isDefined) {
        printF("classic:  ")
      }
      testFunction(test)
    }

    if (isSucceeded(classic) && connectSparkSession.isDefined && !disableSparkConnect(test)) {
      printF("connect:  ")
      inConnect.set(true)
      testFunction(test)
    } else
      classic
  }
}
