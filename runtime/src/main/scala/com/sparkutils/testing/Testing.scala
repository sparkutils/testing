package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Signifies that testing is being done, it should be ignored by users.
 */
object Testing {
  // horrible hack for testing, but at least attempt to make it performant
  private val testingFlag = new AtomicBoolean(false)

  /**
   * Called by TestUtils
   */
  protected[testing] def setTesting() = {
    testingFlag.set(true)
  }

  def testing: Boolean = testingFlag.get

}

/**
 * With the introduction of the 4 runtime folder needs different
 * resolved behaviour on lazytyperef, as such these move here
 * from TestUtils
 */
object SparkVersions {

  lazy val sparkFullVersion = {
    val pos = classOf[Column].getPackage.getSpecificationVersion
    if ((pos eq null) || pos == "0.0") // DBR is always null, Fabric 0.0
      SparkSession.active.version
    else
      pos
  }

  lazy val sparkVersion = sparkFullVersion.split('.').take(2).mkString(".")

  lazy val sparkMajorVersion = sparkFullVersion.split('.').head
}


object ConnectWhenForced {

  val FORCED_CONNECT_PROPERTY_NAME = "SPARKUTILS_TESTING_FORCE_CONNECT"

  /**
   * When SPARKUTILS_TESTING_FORCE_CONNECT is set to true in ENV or Sys properties then None is returned otherwise thunk.
   *
   * This is set/reset by the TestUtils.defaultAndForceConnect helper function
   *
   * @param thunk
   * @tparam T
   * @return
   */
  def someOrForcedConnect[T](thunk: => T): Option[T] =
    if (TestUtilsEnvironment.getConfig(FORCED_CONNECT_PROPERTY_NAME, "false").toBoolean)
      None
    else
      Some(thunk)

}