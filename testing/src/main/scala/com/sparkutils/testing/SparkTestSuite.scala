package com.sparkutils.testing

import org.scalatest.TestSuite

/**
 * Runs tests against both Spark Connect and Spark Classic.  Use the NoSparkConnect tag to disable connect usage for tests.
 */
trait SparkTestSuite extends TestUtils with TestSuite {

  override def withFixture(test: NoArgTest): org.scalatest.Outcome = {
    cleanupOutput()

    val classic =
      {
        inConnect.set(false)
        if (connectSparkSession.isDefined) {
          println("classic:")
        }
        super.withFixture(test)
      }

    if (classic.isSucceeded && connectSparkSession.isDefined && !test.tags.contains(NoSparkConnect.name)) {
      println("connect:")
      inConnect.set(true)
      super.withFixture(test)
    } else
      classic
  }
}
