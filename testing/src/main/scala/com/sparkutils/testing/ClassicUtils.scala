package com.sparkutils.testing

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.sparkutils.testing.Utils.booleanEnv
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession


/**
 * Functionality that only applies to Spark Classic
 */
trait ClassicUtils extends SparkClassicConfig {

  val disableClassicTesting = booleanEnv("SPARKUTILS_DISABLE_CLASSIC_TESTS")

  val classicHostMode = {
    val tmp = System.getenv("SPARKUTILS_SPARK_HOSTS")
    if (tmp eq null)
      "*"
    else
      tmp
  }
  val lambdaSubQueryMode = {
    val tmp = System.getenv("SPARKUTILS_LAMBDA_SUBS")
    if (tmp eq null)
      null
    else
      tmp
  }

  def loggingLevel: String = "ERROR"

  def classicSparkSession: Option[SparkSession] =
    if (disableClassicTesting)
      None
    else
      Some{
        val config = sparkClassicConfig()
        val sparkSession = SparkSession.builder().config(config).getOrCreate()
        // only a visual change
        // sparkSession.conf.set("spark.sql.legacy.castComplexTypesToString.enabled", true)
        sparkSession.sparkContext.setLogLevel(loggingLevel) // set to debug to get actual code lines etc.
        sparkSession
      }

  /**
   * Builds a spark-defaults conf for use on the spawned/forked connect.
   * Override and call to add / override extra config items
   */
  def sparkClassicConfig(): Map[String, String] =
    Set(
      Some("spark.ui.enabled", "false"),
      Some(("spark.master", s"local[$classicHostMode]")),
      if (System.getProperty("os.name").startsWith("Windows"))
        Some(("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName))
      else
        None,
      if (excludeFilters)
        Some(("spark.sql.optimizer.excludedRules","org.apache.spark.sql.catalyst.optimizer.InferFiltersFromGenerate"))
      else
        None,
      Some(("spark.sql.optimizer.nestedSchemaPruning.enabled","true")),
      if (lambdaSubQueryMode ne null)
        Some(("spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions",lambdaSubQueryMode))
      else
        None
    ).flatten.toMap

  val excludeFilters = {
    val tmp = System.getProperty("excludeFilters")
    if (tmp eq null)
      true
    else
      tmp.toBoolean
  }

  // if this blows then debug on CodeGenerator 1294, 1299 and grab code.body
  def forceCodeGen[T](f: => T): T = {
    val codegenMode = CodegenObjectFactoryMode.CODEGEN_ONLY.toString

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
      f
    }
  }

  def forceInterpreted[T](f: => T): T = {
    val codegenMode = CodegenObjectFactoryMode.NO_CODEGEN.toString

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) {
      f
    }
  }

  def inCodegen: Boolean = {
    val r = SQLConf.get.getConfString(SQLConf.CODEGEN_FACTORY_MODE.key)

    r == CodegenObjectFactoryMode.CODEGEN_ONLY.toString ||
      r == CodegenObjectFactoryMode.FALLBACK.toString
  }

  /**
   * Forces resolveWith to be used where possible, Quality only
   */
  val doResolve =
    new ThreadLocal[Boolean] {
      override def initialValue(): Boolean = false
    }

  def doWithResolve[T](f: => T): T =
    try {
      doResolve.set(true)
      f
    } finally {
      doResolve.set(false)
    }

  /**
   * runs the same test with both eval and codegen, then does the same again using resolveWith
   * @param f
   * @tparam T
   * @return
   */
  def evalCodeGens[T](f: => T):(T,T,T,T)  =
    (forceCodeGen(f), forceInterpreted(f), forceCodeGen(doWithResolve(f)), forceInterpreted(doWithResolve(f)))

  /**
   * runs the same test with both eval and codegen
   * @param f
   * @tparam T
   * @return
   */
  def evalCodeGensNoResolve[T](f: => T):(T,T)  =
    (forceCodeGen(f), forceInterpreted(f))

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   */
  protected def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      SparkTestUtils.testStaticConfigKey(k)
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

}
