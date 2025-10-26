package com.sparkutils.testing

import com.sparkutils.testing.SparkVersions.sparkVersion
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.sources.Filter

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.reflect.io.Directory

trait TestUtils extends SessionStrategy with Serializable with ClassicTestUtils {

  // always set
  Testing.setTesting()

  /**
   * returns the current context
   * @return
   */
  def sqlContext: SQLContext = sparkSession.sqlContext // mode specific classicSparkSessionF.sqlContext

  /**
   * returns the current sparkSession, when connect is enabled AND the active session it returns a connect SparkSession, otherwise classic
   * @return
   */
  def sparkSession: SparkSession = {
    if (inConnect.get()) {
      sessions.connect.map(_.sparkSession).orElse(sessions.classic).getOrElse(throw new IllegalStateException("At least one SparkSession must be enabled"))
    } else
      sessions.classic.getOrElse(throw new IllegalStateException("At least one SparkSession must be enabled"))
  }

  /**
   * Simple time capture, use currentTimeMillis as a start, over a minute gets wrapped to minutes in the returned string
   * @param start
   * @return
   */
  def stop(start: Long) = {
    val stop = System.currentTimeMillis()
    if (stop - start > 60000)
      ((stop - start) / 1000 / 60, "m")
    else
      (stop - start, "ms")
  }

  {
    sys.props.put("spark.testing","yes, yes it is")
  }

  lazy val outputDir = SparkTestUtils.ouputDir

  def cleanUp(target: String): Unit = {
    val outdir = new Directory(new java.io.File(target))
    outdir.deleteRecursively()
  }

  def cleanupOutput(): Unit =
    cleanUp(outputDir)

  /**
   * Don't run this test on 2.4 - typically due to not being able to control code gen properly
   */
  def not2_4(thunk: => Unit) =
    if (sparkVersion != "2.4") thunk

  /**
   * Don't run this test on 3.4 - gc's on code gen
   */
  def not3_4(thunk: => Unit) =
    if (sparkVersion != "3.4") thunk

  lazy val sparkVersionNumericMajor = sparkVersion.replace(".","").toInt

  /**
   * Don't run this test on 3.4 or greater - gc's on code gen
   */
  def not3_4_or_above(thunk: => Unit) =
    if (sparkVersionNumericMajor < 34) thunk

  /**
   * Scalar subqueries etc. only work on 3.4 and above
   * @param thunk
   */
  def v3_4_and_above(thunk: => Unit) =
    if (sparkVersionNumericMajor >= 34) thunk

  /**
   * INTERVAL MONTH etc. not supported below 3.2
   * @param thunk
   */
  def v3_2_and_above(thunk: => Unit) =
    if (sparkVersionNumericMajor >= 32) thunk

  /**
   * introduced for the printCode, Add(1,1) now is addExact
   * @param thunk
   */
  def not_4_0_and_above(thunk: => Unit) =
    if (sparkVersionNumericMajor < 40) thunk

  /**
   * introduced for the printCode, Add(1,1) now is addExact
   * @param thunk
   */
  def v4_0_and_above(thunk: => Unit) =
    if (sparkVersionNumericMajor >= 40) thunk

  /**
   * Only run this on 2.4
   * @param thunk
   */
  def only2_4(thunk: => Unit) =
    if (sparkVersion == "2.4") thunk

  /**
   * transform_values and transform_keys pattern match on list only which doesn't work with seq in the _lambda_ param re-writes
   * @param thunk
   */
  def not2_4_or_3_0_or_3_1(thunk: => Unit) =
    if (!Set("2.4", "3.0", "3.1").contains(sparkVersion)) thunk

  /**
   * Assumes /dbfs existance proves running on Databricks
   * Prefer not_Cluster for pure cluster tests.
   * @return
   */
  def onDatabricks: Boolean = TestUtilsEnvironment.onDatabricksFS

  /**
   * Should prefer not_Cluster as it works for fabric as well
   *
   * Don't run this test on Databricks - due to either running in a cluster or, in the case of trying for force soe's etc
   * because Databricks defaults and Codegen are different
   */
  @deprecated
  def not_Databricks(thunk: => Unit) =
    if (!onDatabricks) thunk

  /**
   * Do not run the test unless shouldRunClusterTests is true
   */
  def not_Cluster(thunk: => Unit): Unit =
    if (TestUtilsEnvironment.shouldRunClusterTests) thunk

}

object TestUtils {

  /**
   * Checks for an exception, then it's cause(s) for f being true
   * @param t
   * @param f
   * @return
   */
  def anyCauseHas(t: Throwable, f: Throwable => Boolean): Boolean =
    if (f(t))
      true
    else
      if (t.getCause ne null)
        anyCauseHas(t.getCause, f)
      else
        false

  /**
   * If setShouldDebugLog is true run the thunk
   * @param thunk
   */
  def debug(thunk: => Unit): Unit =
    TestUtilsEnvironment.debug(thunk)

}

object TestUtilsEnvironment {

  /**
   * Checks if the master is local, if so it's likely tests, if spark.master is not defined it's false
   */
  def isLocal(sparkSession: SparkSession): Boolean =
    sparkSession.conf.getOption("spark.master").fold(false)(_.toLowerCase().startsWith("local"))

  lazy val onDatabricksFS = {
    val dbfs = new File("/dbfs")
    dbfs.exists
  }

  /**
   * Databricks config is found on all delta running platforms, synapse/fabric _so far_ is not.  If any config
   * starts with spark.synapse it is assumed to be running on synapse/fabric
   * @param sparkSession
   * @return
   */
  def onFabricOrSynapse(sparkSession: SparkSession): Boolean =
    sparkSession.conf.getAll.exists(p => p._1.startsWith("spark.synapse"))

  private var shouldRunClusterTestsWasSet = false
  private var shouldRunClusterTestsV = true

  /** allow test usage on non-build environments */
  def setshouldRunClusterTests(shouldClose: Boolean): Unit = {
    this.shouldRunClusterTestsV = shouldClose
    shouldRunClusterTestsWasSet = true
  }

  lazy val shouldRunClusterTests = shouldRunClusterTestsV

  private var shouldDebugLogWasSet = false
  private var shouldDebugLogv = false

  /** allow test usage on non-build environments */
  def setShouldDebugLog(shouldDebugLog: Boolean): Unit = {
    shouldDebugLogv = shouldDebugLog
    shouldDebugLogWasSet = true
  }

  lazy val shouldDebugLog = shouldDebugLogv

  /**
   * Only called from with in the test suite assume if the original values are not per default then they have been
   * set that way.
   * If shouldCloseSession is false and debug logging was not explicitly enabled then debug logging is disabled (protecting against any accidental defaulting of true).
   * @param sparkSession if null it's assumed to be running locally, defaults win
   */
  def setupDefaults(sparkSession: SparkSession): Unit =
    if (sparkSession ne null) {
      if (!shouldRunClusterTestsWasSet) {
        setshouldRunClusterTests( isLocal(sparkSession) && !(onDatabricksFS || onFabricOrSynapse(sparkSession)) )
      }
      if (!shouldDebugLogWasSet) {
        if (!shouldRunClusterTestsV) {
          setShouldDebugLog(false)
        }
      }
    }

  def setupDefaultsViaCurrentSession(): Unit =
    SparkSession.getActiveSession.foreach(setupDefaults)

  def debug(thunk: => Unit): Unit =
    if (shouldDebugLog) {
      thunk
    }
}