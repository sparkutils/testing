package com.sparkutils.testing

import com.sparkutils.testing.SparkVersions.sparkVersion
import org.apache.spark.sql.catalyst.expressions.Expression
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
  def sparkSession: SparkSession =
    if (inConnect.get())
      sessions.connect.map(_.sparkSession).orElse(sessions.classic).getOrElse(throw new IllegalStateException("At least one SparkSession must be enabled"))
    else
      sessions.classic.getOrElse(throw new IllegalStateException("At least one SparkSession must be enabled"))

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
   * transform_values and transform_keys pattern match on list only which doesn't work with seq in the _lambda_ param re-writes
   * @param thunk
   */
  def not3_0_or_3_1(thunk: => Unit) =
    if (!Set("3.0", "3.1").contains(sparkVersion)) thunk

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

  /**
   * When inConnect is true the thunk is run once with SPARKUTILS_TESTING_FORCE_CONNECT=false and then
   * with SPARKUTILS_TESTING_FORCE_CONNECT=true enabling functions using ConnectWhenForced.someOrForcedConnect to be tested.
   * @param thunk
   */
  def defaultAndforceConnect(thunk: => Unit): Unit =
    if (inConnect.get()) {
      val prev = System.getProperty(ConnectWhenForced.FORCED_CONNECT_PROPERTY_NAME) // only possible to change property
      try {
        System.setProperty(ConnectWhenForced.FORCED_CONNECT_PROPERTY_NAME, "false")
        thunk
        System.setProperty(ConnectWhenForced.FORCED_CONNECT_PROPERTY_NAME, "true")
        thunk
      } finally {
        System.setProperty(ConnectWhenForced.FORCED_CONNECT_PROPERTY_NAME, prev)
      }
    } else {
      thunk // using whatever is current
    }

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

  /**   * If setShouldDebugLog is true run the thunk
   * @param thunk
   */
  def debug(thunk: => Unit): Unit =
    TestUtilsEnvironment.debug(thunk)

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    ClassicSparkTestUtils.resolveBuiltinOrTempFunction(sparkSession)(name, exps)

  def enumToScala[A](enum: java.util.Enumeration[A]) = {
    import scala.collection.JavaConverters._

    enumerationAsScalaIterator(enum)
  }

  def loadsOf(thunk: => Unit, runs: Int = 3000): Unit = {
    var passed = 0
    for{ i <- 0 until runs }{
      try {
        thunk
        passed += 1
      } catch {
        case e: org.scalatest.exceptions.TestFailedException => println("failed "+e.getMessage())
        case t: Throwable => println("failed unexpectedly "+t.getMessage())
      }
    }
    assert(passed == runs, "Should have passed all of them, nothing has changed in between runs")
  }
}
