package com.sparkutils.testing

import org.apache.spark.sql.SparkSession

import java.io.File


object TestUtilsEnvironment {

  /**
   * first attempts to get the system env, then system java property then sqlconf
   * @param name
   * @return
   */
  def getConfig(name: String, default: String = "") = try {
    val res = System.getenv(name)
    if (res ne null)
      res
    else {
      val sp = System.getProperty(name)
      if (sp ne null)
        sp
      else
        SQLConf.get.getConfString(name, default)
    }
  } catch {
    case _: Throwable => default
  }

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
  def setShouldRunClusterTests(shouldRunClusterTests: Boolean): Unit = {
    this.shouldRunClusterTestsV = shouldRunClusterTests
    shouldRunClusterTestsWasSet = true
  }

  lazy val shouldRunClusterTests: Boolean = shouldRunClusterTestsV

  private var shouldDebugLogWasSet = false
  private var shouldDebugLogv = false

  /** allow test usage on non-build environments */
  def setShouldDebugLog(shouldDebugLog: Boolean): Unit = {
    shouldDebugLogv = shouldDebugLog
    shouldDebugLogWasSet = true
  }

  lazy val shouldDebugLog: Boolean = shouldDebugLogv

  /**
   * Only called from with in the test suite assume if the original values are not per default then they have been
   * set that way.
   * If setShouldRunClusterTests is false and debug logging was not explicitly enabled then debug logging is disabled (protecting against any accidental defaulting of true).
   * @param sparkSession if null it's assumed to be running locally, defaults win
   */
  def setupDefaults(sparkSession: SparkSession): Unit =
    if (sparkSession ne null) {
      if (!shouldRunClusterTestsWasSet) {
        setShouldRunClusterTests( isLocal(sparkSession) && !(onDatabricksFS || onFabricOrSynapse(sparkSession)) )
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