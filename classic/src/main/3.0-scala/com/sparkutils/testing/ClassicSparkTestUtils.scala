package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

object ClassicSparkTestUtils {

  def testStaticConfigKey(k: String) =
    if (SQLConf.staticConfKeys.contains(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    Some(sparkSession.sessionState.catalog.lookupFunction(FunctionIdentifier(name), exps))

}
