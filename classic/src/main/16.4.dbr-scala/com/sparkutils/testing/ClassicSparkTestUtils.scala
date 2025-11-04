package com.sparkutils.testing

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

object ClassicSparkTestUtils {

  def testStaticConfigKey(k: String) =
    if (SQLConf.isStaticConfigKey(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    sparkSession.sessionState.catalog.resolveBuiltinOrTempFunction(name, exps)
  import org.apache.spark.sql.execution.SparkPlan
  import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

  def getCorrectPlan(sparkPlan: SparkPlan): SparkPlan =
    if (sparkPlan.children.isEmpty)
      // assume it's AQE
      sparkPlan match {
        case aq: AdaptiveSparkPlanExec => aq.initialPlan
        case _ => sparkPlan
      }
    else
      sparkPlan

  /**
   * Returns the correct executed plan from this dataset.
   * This is appropriate to chain with getPushDowns if running on both connect and classic
   * @param dataset
   * @return None if running on connect
   */
  def getExecutedPlan(dataset: Dataset[_]): Option[SparkPlan] =
    Some(getCorrectPlan(dataset.queryExecution.executedPlan))

}
