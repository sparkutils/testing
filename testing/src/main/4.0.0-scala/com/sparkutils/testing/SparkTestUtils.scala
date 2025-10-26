package com.sparkutils.testing

import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.sql.{SparkConnectServerUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connect.test.SparkConnectServerUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.{FileSourceScanExec, LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

case class AnalysisException(message: String) extends Exception(message)

object SparkTestUtils {

  /**
   * 9.1.dbr oss hofs cannot work in Quality tests, but on other versions does
   */
  val skipHofs = false

  def testStaticConfigKey(k: String): Unit =
    ClassicSparkTestUtils.testStaticConfigKey(k)

  protected var tpath = new AtomicReference[String]("./target/testData")

  def ouputDir = tpath.get

  def setPath(newPath: String) = {
    tpath.set(newPath)
  }

  def path(suffix: String) = s"${tpath.get}/$suffix"

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    ClassicSparkTestUtils.resolveBuiltinOrTempFunction(sparkSession)(name, exps)

  def enumToScala[A](enum: java.util.Enumeration[A]) = {
    import scala.collection.JavaConverters._

    enumerationAsScalaIterator(enum)
  }

}
