package org.apache.spark.sql.sparkutils.testing

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

object PlanHelper {


  def getCorrectPlan(sparkPlan: SparkPlan): SparkPlan =
    if (sparkPlan.children.isEmpty)
      // assume it's AQE
      sparkPlan match {
        case aq: AdaptiveSparkPlanExec => aq.initialPlan
        case _ => sparkPlan
      }
    else
      sparkPlan

}
