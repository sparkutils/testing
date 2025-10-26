package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf

case class AnalysisException(message: String) extends Exception(message)

object ClassicSparkTestUtils {

  def resolveBuiltinOrTempFunction(sparkSession: SparkSession)(name: String, exps: Seq[Expression]): Option[Expression] =
    sparkSession.sessionState.catalog.resolveBuiltinOrTempFunction(name, exps)

  def testStaticConfigKey(k: String) =
    if (SQLConf.isStaticConfigKey(k)) {
      throw new AnalysisException(s"Cannot modify the value of a static config: $k")
    }

}
