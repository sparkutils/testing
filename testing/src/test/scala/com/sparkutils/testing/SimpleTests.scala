package com.sparkutils.testing

import com.sparkutils.testing.sessionStrategies.NewSessionEverySuiteWithSharedConnectServer
import org.scalatest.{FunSuite, Matchers}

class SimpleTests extends FunSuite with SparkTestSuite with NewSessionEverySuiteWithSharedConnectServer with Matchers {
  override protected def currentSessionsHolder: SessionsStateHolder = GlobalSession

  test("verify simple queries work") {
    val s = sparkSession
    import s.implicits._
    sparkSession.sql("select 1+1").as[Int].collect().head shouldBe 2
  }
}

object GlobalSession extends SessionsStateHolder {

  var current: Sessions = _

  override def setSessions(sessions: => Sessions): Unit =
    if (current eq null) {
      current = sessions
    }

  override def getSessions: Sessions = current

}