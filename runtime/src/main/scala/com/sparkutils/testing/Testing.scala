package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Signifies that testing is being done, it should be ignored by users.
 */
object Testing {
  // horrible hack for testing, but at least attempt to make it performant
  private val testingFlag = new AtomicBoolean(false)

  /**
   * Should not be used by users but currently (0.0.2) only forces re-evaluation of the quality.lambdaHandlers configuration rather than caching once.
   */
  protected[sparkutils] def setTesting() = {
    testingFlag.set(true)
  }

  def testing: Boolean = testingFlag.get

  /**
   * Should not be called by users of the library and is provided for testing support only
   * @param thunk
   */
  protected[sparkutils] def test(thunk: => Unit): Unit = try {
    setTesting()
    thunk
  } finally {
    testingFlag.set(false)
  }
}

/**
 * With the introduction of the 4 runtime folder needs different
 * resolved behaviour on lazytyperef, as such these move here
 * from TestUtils
 */
object SparkVersions {

  lazy val sparkFullVersion = {
    val pos = classOf[Expression].getPackage.getSpecificationVersion
    if ((pos eq null) || pos == "0.0") // DBR is always null, Fabric 0.0
      SparkSession.active.version
    else
      pos
  }

  lazy val sparkVersion = sparkFullVersion.split('.').take(2).mkString(".")

  lazy val sparkMajorVersion = sparkFullVersion.split('.').head
}