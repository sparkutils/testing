/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import com.sparkutils.testing.ConnectSession
import com.sparkutils.testing.SparkTestUtils.{DEBUG_CONNECT_LOGS_SYS, FLAT_JVM_OPTION, MAIN_CLASSPATH, booleanEnvOrProp, classPathJars, connectServerJars, stringEnvOrProp, testClassPaths}
import com.sparkutils.testing.TestUtilsEnvironment.{onDatabricksFS, onFabricOrSynapse}
import org.apache.spark.{SparkBuildInfo, sql}
import org.apache.spark.sql.connect.SparkSession
import org.apache.spark.sql.connect.client.{RetryPolicy, SparkConnectClient}
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.sql.connect.test.IntegrationTestUtils
import org.apache.spark.sql.connect.test.IntegrationTestUtils._
import org.apache.spark.util.ArrayImplicits._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Waiters.timeout
import org.scalatest.time.SpanSugar._

import java.io.{File, IOException, OutputStream}
import java.lang.ProcessBuilder.Redirect
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/*
Taken directly from the Spark 4 connect codebase, with additional config insertion to allow for SparkSessionExtensions
and custom expression code to be tested.  The server is run directly via java not via spark-connect.sh
 */

/**
 * An util class to start a local spark connect server in a different process for local E2E tests.
 * Pre-running the tests, the spark connect artifact needs to be built using e.g. `build/sbt
 * package`. It is designed to start the server once but shared by all tests.
 *
 * Set system property `spark.test.home` or env variable `SPARK_HOME` if the test is not executed
 * from the Spark project top folder. Set system property `spark.debug.sc.jvm.client=true` or
 * environment variable `SPARK_DEBUG_SC_JVM_CLIENT=true` to print the server process output in the
 * console to debug server start stop problems.
 */
case class SparkConnectServerUtils(config: Map[String, String]) {

  private lazy val classItems = config.getOrElse(MAIN_CLASSPATH,"")
  private lazy val (jvmOptsExtraBase, extraMainConfigItemsBase) =
    ((config - MAIN_CLASSPATH).filter{
      p => p._1.startsWith(FLAT_JVM_OPTION)
    }, (config - MAIN_CLASSPATH).filterNot{
      p => p._1.startsWith(FLAT_JVM_OPTION)
    })
  private lazy val jvmOptsExtra = jvmOptsExtraBase.values
  private lazy val extraMainConfigItems = extraMainConfigItemsBase.map(p => s"-D${p._1}=${p._2}")

  private val jvmOpts = Seq(
    "-ea",
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  )

  // The equivalent command to start the connect server via command line:
  // bin/spark-shell --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

  // Server port
  val port: Int =
    ConnectCommon.CONNECT_GRPC_BINDING_PORT + scala.util.Random.nextInt(1000)

  @volatile private var stopped = false

  private var consoleOut: OutputStream = _
  private val serverStopCommand = "q"
  @volatile private var exitVal = 0

  private lazy val sparkConnect: java.lang.Process = {
    debug("Starting the Spark Connect Server...")
//    val connectJar =
  //    findJar("sql/connect/server", "spark-connect-assembly", "spark-connect").getCanonicalPath

    val command = Seq.newBuilder[String]
    command += s"${System.getProperty("java.home")}/bin/java"
    command ++= jvmOpts
    command ++= jvmOptsExtra
    val classPathOriginal = connectServerJars.mkString(File.pathSeparatorChar.toString)

    val classPath = (classPathOriginal + (
      if (classItems.nonEmpty)
        s"${File.pathSeparatorChar}$classItems"
      else
        ""
      )
    )
    //command += "--driver-class-path" += connectJar
    command += s"-Dspark.connect.grpc.binding.port=$port"
    command ++= testConfigs
//    command ++= debugConfigs
    command ++= extraMainConfigItems
    command ++= Seq(
      "-Dspark.master=local[*]",
      "-Dspark.app.name=test"
    )
    command += "org.apache.spark.sql.connect.service.SparkConnectServer"
      //"org.apache.spark.sql.connect.SimpleSparkConnectService"

    // command += connectJar
    val builder = new ProcessBuilder(command.result(): _*)
    builder.directory(new File(sparkHome))
    val environment = builder.environment()
    environment.put("SPARK_USER", "test")
    environment.put("SPARK_MASTER", "local[*]") // doesn't take it fully via -D
    environment.put("CLASSPATH", classPath)
    environment.remove("SPARK_DIST_CLASSPATH")
    if (isDebug) {
      builder.redirectError(Redirect.INHERIT)
      builder.redirectOutput(Redirect.INHERIT)
    } else {
      builder.redirectError(Redirect.DISCARD)
      builder.redirectOutput(Redirect.DISCARD)
    }

    val process = builder.start()
    consoleOut = process.getOutputStream
    // Adding JVM shutdown hook
    sys.addShutdownHook(stop())
    process
  }

  /**
   * As one shared spark will be started for all E2E tests, for tests that needs some special
   * configs, we add them here
   */
  private def testConfigs: Seq[String] = {
    val catalogImplementation = if (IntegrationTestUtils.isSparkHiveJarAvailable) {
      "hive"
    } else {
      // scalastyle:off println
      /*println(
        "Will start Spark Connect server with `spark.sql.catalogImplementation=in-memory`, " +
          "some tests that rely on Hive will be ignored. If you don't want to skip them:\n" +
          "1. Test with maven: run `build/mvn install -DskipTests -Phive` before testing\n" +
          "2. Test with sbt: run test with `-Phive` profile")*/
      // scalastyle:on println
      // SPARK-43647: Proactively cleaning the `classes` and `test-classes` dir of hive
      // module to avoid unexpected loading of `DataSourceRegister` in hive module during
      // testing without `-Phive` profile.
      IntegrationTestUtils.cleanUpHiveClassesDirIfNeeded()
      "in-memory"
    }
    Seq(
      // Use InMemoryTableCatalog for V2 writer tests
      "spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog",
      // Try to use the hive catalog, fallback to in-memory if it is not there.
      "spark.sql.catalogImplementation=" + catalogImplementation,
      // Make the server terminate reattachable streams every 1 second and 123 bytes,
      // to make the tests exercise reattach.
      "spark.connect.execute.reattachable.senderMaxStreamDuration=1s",
      "spark.connect.execute.reattachable.senderMaxStreamSize=123",
      // Testing SPARK-49673, setting maxBatchSize to 10MiB
      s"spark.connect.grpc.arrow.maxBatchSize=${10 * 1024 * 1024}",
      // Disable UI
      "spark.ui.enabled=false").map(v => s"-D$v")
  }

  def start(): Unit = {
    assert(!stopped)
    sparkConnect
  }

  def stop(): Int = {
    if (!stopped) {
      stopped = true
      debug("Stopping the Spark Connect Server...")
      exitVal =
        try {
          consoleOut.write(serverStopCommand.getBytes)
          consoleOut.flush()
          consoleOut.close()
          if (!sparkConnect.waitFor(2, TimeUnit.SECONDS)) {
            sparkConnect.destroyForcibly().waitFor(2, TimeUnit.SECONDS)
          }
          val code = sparkConnect.exitValue()
          debug(s"Spark Connect Server is stopped with exit code: $code")
          code
        } catch {
          case e: IOException if e.getMessage.contains("Stream closed") =>
            -1
          case e: Throwable =>
            debug(e)
            sparkConnect.destroyForcibly()
            throw e
        }
    }
    exitVal
  }

}

object SparkConnectServerUtils {

  def syncTestDependencies(spark: SparkSession): Unit = {
    // add all test dirs in the classpath
    testClassPaths.map(e => Paths.get(e)).foreach {
      testClassesPath =>
        spark.client.artifactManager.addClassDir(testClassesPath)
    }

    // We need scalatest & scalactic on the session's classpath to make the tests work.
    val jars = System
      .getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter { e: String =>
        val fileName = e.substring(e.lastIndexOf(File.separatorChar) + 1)
        fileName.endsWith(".jar") &&
          (fileName.startsWith("scalatest") || fileName.startsWith("scalactic") ||
            (fileName.startsWith("spark-catalyst") && fileName.endsWith("-tests")))
      }
      .map(e => Paths.get(e).toUri)
    spark.client.artifactManager.addArtifacts(jars.toImmutableArraySeq)
  }

  def createSparkSession(port: Int, clientConfig: Map[String, String]): SparkSession = {

    val spark = SparkSession
      .builder()
      .config(clientConfig)
      .client(
        SparkConnectClient
          .builder()
          .userId("test")
          .port(port)
          .retryPolicy(RetryPolicy
            .defaultPolicy()
            .copy(maxRetries = Some(10), maxBackoff = Some(FiniteDuration(30, "s"))))
          .build())
      .create()

    // Execute an RPC which will get retried until the server is up.
    eventually(timeout(1.minute)) {
      assert(spark.version == SparkBuildInfo.spark_version)
    }

    // Auto-sync dependencies.
    syncTestDependencies(spark)

    spark
  }

  /**
   * The extra indirection here with SparkSession.getActiveSession is to allow running on Databricks/Fabric in
   * shared / isolated mode with the provided SparkSession.
   * As this _in the junit testing scenario_ could be a classic session it is first checked for being a connect session.
   *
   * @param serverConfig
   * @param clientConfig
   * @return
   */
  def localConnectServerForTesting(serverConfig: Map[String, String], clientConfig: Map[String, String]): Option[ConnectSession] = {
    val spawnConnect =
      // on Databricks (and possibly fabric) this will be provided for notebooks and for testing remotely
      SparkSession.getActiveSession.forall {
        s =>
          !classOf[org.apache.spark.sql.connect.SparkSession].isAssignableFrom(s.getClass)
      }

    // doing a SparkSession.builder.config("spark.api.mode", "connect").getOrCreate() works fine on
    // classic non-shared databricks setup, should also be the case for a normal submitted job on OSS / Fabric
    val localOnly = booleanEnvOrProp("SPARKUTILS_TESTING_USE_LOCAL_CONNECT")

    // despite the function name, we may want to test against remote servers
    val connectURL = stringEnvOrProp("SPARK_REMOTE")

    Some(
      if (connectURL ne null)
        ExistingSession(SparkSession.builder.config(clientConfig).getOrCreate())
      else
        if (spawnConnect) {
          // if there is a forced local connect, e.g. running 4.0.0 full shades on a later Fabric 1.4 that doesn't force a
          // connect setup, we have a way out
          if (localOnly)
            ExistingSession(SparkSession.builder.config("spark.api.mode", "connect").getOrCreate())
          else
            new ConnectSession {

               val filter =
                serverConfig.get(DEBUG_CONNECT_LOGS_SYS).exists { v =>
                  System.setProperty(DEBUG_CONNECT_LOGS_SYS, v)
                  true
                }

              val utils = SparkConnectServerUtils(
                if (filter)
                  serverConfig - DEBUG_CONNECT_LOGS_SYS
                else
                  serverConfig
              )

              val th = System.getProperty("spark.test.home")
              if (th eq null) {
                new File("./testing_connect_tmp").mkdirs()
                System.setProperty("spark.test.home","./testing_connect_tmp")
              }


              utils.start()

              private def createSparkSession: SparkSession = SparkConnectServerUtils.createSparkSession(utils.port, clientConfig)

              private var _sparkSession: SparkSession = createSparkSession

              override def sparkSession: sql.SparkSession = _sparkSession

              override def stopServer(): Unit = utils.stop()

              override def resetSession(): Unit =
                if (!(onFabricOrSynapse(_sparkSession) || onDatabricksFS)) {
                  if (sparkSession.isUsable) {
                    sparkSession.stop()
                  }
                  _sparkSession = createSparkSession
                } // else leave as is, no reset to do
            }
        } else
          ExistingSession(SparkSession.active)
    )
  }

  case class ExistingSession(existingSparkSession: sql.SparkSession) extends ConnectSession {
    /**
     * only one as we were given it
     */
    override val sparkSession: sql.SparkSession = existingSparkSession

    override def resetSession(): Unit = {}

    override def stopServer(): Unit = {}
  }
}