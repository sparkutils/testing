package com.sparkutils.testing

import com.sparkutils.shim.expressions.UnresolvedFunction4
import com.sparkutils.testing.ClassicTestUtils.getPushDowns
import com.sparkutils.testing.Utils.testClassesPathsConfig
import com.sparkutils.testing.sessionStrategies.NewSessionEverySuiteWithSharedConnectServer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, ShimUtils, SparkSessionExtensions}
import org.scalatest.{FunSuite, Matchers}

case class Echo(child: Expression) extends UnaryExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = child.genCode(ctx)

  override def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = child.eval(input)

  protected def withNewChildInternal(newChild: Expression): Expression = Echo(newChild)
}

object Echo {
  def apply(child: Column): Column =
    ShimUtils.column(UnresolvedFunction4("sparkutils_echo", Seq(ShimUtils.expression(child)), false))
}

class EchoSparkExtension extends ((SparkSessionExtensions) => Unit) with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    val func = ShimUtils.registerFunctionViaExtension(extensions) _
    func("sparkutils_echo", exps => Echo(exps.head))
  }

}


class SimpleTests extends FunSuite with SparkTestSuite with NewSessionEverySuiteWithSharedConnectServer with Matchers {
  override protected def currentSessionsHolder: SessionsStateHolder = GlobalSession

  // force through the test class paths AND spark extension
  override def sparkConnectServerConfig(): Map[String, String] =
    super.sparkConnectServerConfig() + testClassesPathsConfig +
      (("spark.sql.extensions", classOf[EchoSparkExtension].getName))

  test("verify simple queries work") {
    val s = sparkSession
    import s.implicits._
    sparkSession.sql("select 1+1").as[Int].collect().head shouldBe 2
  }

  test("verify pushdowns") { classicOnly {
    sparkSession.range(100).write.parquet(outputDir + "/ids")
    val df = sparkSession.read.parquet(outputDir + "/ids").filter("id < 40")
    val pushdowns = getPushDowns( df )
    pushdowns.nonEmpty shouldBe true
  } }

  test("verify custom expressions") { evalCodeGens{
    classicOnly {
      // perform locally for classic, but connect will use the extension
      ClassicTestUtils.registerFunction(sparkSession)("sparkutils_echo", exps => Echo(exps.head))
    }
    val s = sparkSession
    import s.implicits._
    sparkSession.sql("select sparkutils_echo(1)").as[Int].collect().head shouldBe 1
  } }
}

object GlobalSession extends SessionsStateHolder {

  var current: Sessions = _

  override def setSessions(sessions: => Sessions): Unit =
    if (current eq null) {
      current = sessions
    }

  override def getSessions: Sessions = current

}