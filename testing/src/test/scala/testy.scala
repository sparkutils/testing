import com.sparkutils.testing.SparkTestUtils

object testy {
  def main(args: Array[String]): Unit = {
    val oConnectSession = SparkTestUtils.localConnectServerForTesting(Map.empty)
    oConnectSession.foreach{
      connectSession =>
       val session = connectSession.sparkSession
       import session.implicits._
       val res = session.sql("select 1+1").as[Int].collect()
       res.foreach(println)
    }
  }
}
