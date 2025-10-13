package com.sparkutils.testing

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Functionality that only applies to Spark Classic
 */
trait ClassicUtils {

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

  def classicSparkSessionF: SparkSession = {
    val sparkSession = registerFS(SparkSession.builder()).config("spark.master", s"local[$classicHostMode]").config("spark.ui.enabled", false).getOrCreate()
    if (excludeFilters) {
      sparkSession.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.InferFiltersFromGenerate")
    }

    sparkSession.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", true)
    if (lambdaSubQueryMode ne null) {
      sparkSession.conf.set("spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions", lambdaSubQueryMode)
    }
    // only a visual change
    // sparkSession.conf.set("spark.sql.legacy.castComplexTypesToString.enabled", true)
    sparkSession.sparkContext.setLogLevel(loggingLevel) // set to debug to get actual code lines etc.
    sparkSession
  }

  val excludeFilters = {
    val tmp = System.getProperty("excludeFilters")
    if (tmp eq null)
      true
    else
      tmp.toBoolean
  }

  /**
   * Allows bare naked to be used instead of winutils for testing / dev
   */
  def registerFS(sparkSessionBuilder: SparkSession.Builder): SparkSession.Builder =
    if (System.getProperty("os.name").startsWith("Windows"))
      sparkSessionBuilder.config("spark.hadoop.fs.file.impl",classOf[BareLocalFileSystem].getName)
    else
      sparkSessionBuilder

  lazy val classicSparkSession: SparkSession = classicSparkSessionF
  lazy val classicSqlContext: SQLContext = classicSparkSessionF.sqlContext



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
