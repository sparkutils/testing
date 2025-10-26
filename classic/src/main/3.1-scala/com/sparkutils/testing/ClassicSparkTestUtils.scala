package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf

object ClassicSparkTestUtils {

  def testStaticConfigKey(k: String) =
    if (SQLConf.staticConfKeys.contains(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

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

}
