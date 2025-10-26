package com.sparkutils.testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import java.util.concurrent.atomic.AtomicReference

object SparkTestUtils {

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
