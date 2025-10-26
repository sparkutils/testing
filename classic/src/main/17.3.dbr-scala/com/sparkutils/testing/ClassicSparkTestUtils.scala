package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf

import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.sql.SparkSession

object ClassicSparkTestUtils {

  def testStaticConfigKey(k: String) =
    if (SQLConf.isStaticConfigKey(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

}
