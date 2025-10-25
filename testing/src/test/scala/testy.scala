import com.sparkutils.testing.{ClassicUtils, SparkConnectConfig, SparkTestUtils}

object testy {
  def main(args: Array[String]): Unit = {
    val oConnectSession = SparkTestUtils.localConnectServerForTesting(new SparkConnectConfig{}.sparkConnectServerConfig(),Map.empty)
    oConnectSession.foreach{
      connectSession =>
        val session = connectSession.sparkSession
        import session.implicits._
        val res = session.sql("select 1+1").as[Int].collect()
        res.foreach(println)
        connectSession.stopServer()
    }

    val classic = new ClassicUtils {

    }

    classic.classicSparkSession.foreach{
      session =>
        import session.implicits._
        val res = session.sql("select 1+1").as[Int].collect()
        res.foreach(println)
        session.close()
    }

  }
}
