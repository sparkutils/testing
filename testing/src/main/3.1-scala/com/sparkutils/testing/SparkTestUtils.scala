package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.sql.SparkSession

case class AnalysisException(message: String) extends Exception(message)

object SparkTestUtils {

  /**
   * 9.1.dbr oss hofs cannot work in Quality tests, but on other versions does
   */
  val skipHofs = false

  protected var tpath = new AtomicReference[String]("./target/testData")

  def ouputDir = tpath.get


  def setPath(newPath: String) = {
    tpath.set(newPath)
  }

  def path(suffix: String) = s"${tpath.get}/$suffix"

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    Some(sparkSession.sessionState.catalog.lookupFunction(FunctionIdentifier(name), exps))

  val field = classOf[AdaptiveSparkPlanExec].getDeclaredField("initialPlan")
  field.setAccessible(true)

  def getCorrectPlan(sparkPlan: SparkPlan): SparkPlan =
    if (sparkPlan.children.isEmpty)
      // assume it's AQE
      try {
        (field.get(sparkPlan).asInstanceOf[SparkPlan])
      } catch {
        case _: Throwable => sparkPlan
      }
      else
      sparkPlan

  def enumToScala[A](enum: java.util.Enumeration[A]) = {
    import scala.collection.JavaConverters._

    enumerationAsScalaIterator(enum)
  }

}
