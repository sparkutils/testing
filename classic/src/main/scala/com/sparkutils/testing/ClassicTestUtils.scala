package com.sparkutils.testing

import com.sparkutils.testing.ClassicSparkTestUtils.getCorrectPlan
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.sources.Filter

import java.util.concurrent.atomic.AtomicBoolean

trait ClassicTestUtils extends Serializable {

  protected[testing] val inConnect = new AtomicBoolean(false)

  def sparkSession: SparkSession

  val sparkVersionNumericMajor: Int

  def testPlan(logicalPlanRule: org.apache.spark.sql.catalyst.rules.Rule[LogicalPlan], secondRunWithoutPlan: Boolean = true, disable: Int => Boolean = _ => false)(thunk: => Unit): Unit = {
    val cur = SparkSession.getActiveSession.get.experimental.extraOptimizations
    try{
      if (!disable(sparkVersionNumericMajor)) {
        SparkSession.getActiveSession.get.experimental.extraOptimizations = SparkSession.getActiveSession.get.experimental.extraOptimizations :+ logicalPlanRule
      }
      thunk
    } finally {
      SparkSession.getActiveSession.get.experimental.extraOptimizations = cur
      if (secondRunWithoutPlan) {
        thunk // re-run it
      }
    }
  }

  /**
   * Only run when the extension is enabled and in classic mode
   */
  def onlyWithExtension(thunk: => Unit): Unit = if (!inConnect.get()) {
    val extensions = sparkSession.sparkContext.getConf.get("spark.sql.extensions","")
    if (extensions.indexOf("com.sparkutils.quality.impl.extension.QualitySparkExtension") > -1) {
      thunk
    }
  }
}

object ClassicTestUtils {

  /**
   * Gets pushdowns from a dataset
   * @return
   */
  def getPushDowns[T](dataset: Dataset[T]): Seq[Filter] =
    getPushDowns(dataset.queryExecution.executedPlan)

  /**
   * Gets pushdowns from a FileSourceScanExec from a plan
   * @param sparkPlan
   * @return
   */
  def getPushDowns(sparkPlan: SparkPlan): Seq[Filter] =
    getCorrectPlan(sparkPlan).collect {
      case fs: FileSourceScanExec =>
        import scala.reflect.runtime.{universe => ru}

        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
        val instanceMirror = runtimeMirror.reflect(fs)
        val getter = ru.typeOf[FileSourceScanExec].member(ru.TermName("pushedDownFilters")).asTerm.getter
        val m = instanceMirror.reflectMethod(getter.asMethod)
        val res = m.apply(fs).asInstanceOf[Seq[Filter]]

        res
    }.flatten

}
