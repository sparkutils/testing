package com.sparkutils.testing

import org.apache.spark.sql.SparkSession

import java.io.File
import java.util.concurrent.atomic.AtomicReference
import scala.util.Try
import scala.jdk.CollectionConverters._

object SparkTestUtils {

  def parseBoolean(s: String): Option[Boolean] =
    if (s eq null)
      None
    else
      Try {
        Some(s.toBoolean)
      }.getOrElse(None)

  def booleanEnvOrProp(env: String, default: Boolean = false): Boolean =
    parseBoolean( System.getenv(env) ).orElse(
      parseBoolean( System.getProperty(env) )
    ).getOrElse(default)

  def stringEnvOrProp(env: String, default: String = null): String =
    Option( System.getenv(env) ).orElse(
      Option( System.getProperty(env) )
    ).getOrElse(default)

  def configFromPrefix(prefix: String): Map[String, String] = {
    def hasPrefix(p: (String, String)) =
      if (p._1.startsWith(prefix))
        Some((p._1.drop(prefix.length), p._2))
      else
        None

    (System.getenv().asScala.flatMap(hasPrefix) ++
      System.getProperties.asScala.flatMap(hasPrefix)).toMap
  }


  /**
   * If there is a sparkSession already _and_ it's connect - then default to true, otherwise false.
   *
   * The environment or System.property SPARKUTILS_DISABLE_CLASSIC_TESTS can override the default
   */
  val disableClassicTesting: Boolean = booleanEnvOrProp("SPARKUTILS_DISABLE_CLASSIC_TESTS",
    SparkSession.getActiveSession.exists { s =>
      s.getClass.getName == "org.apache.spark.sql.connect.SparkSession"
    }
  )

  /**
   * All non jars on the classpath
   */
  lazy val classPathDirs: Seq[String] =
    System
      .getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter { e: String =>
        !e.endsWith(".jar") && (new File(e).isDirectory)
      }

  /**
   * All jars on the classpath, these are assumed to be safe to use for connect servers
   */
  lazy val classPathJars: Seq[String] =
    System
      .getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter { e: String =>
        e.endsWith(".jar")
      }

  /**
   * Removes connect client jars to allow the correct runtime class to be chosen
   */
  lazy val connectServerJars: Seq[String] =
    classPathJars.filterNot(_.contains("spark-connect-client-jvm"))

  /**
   * /test-classes directories
   */
  lazy val testClassPaths: Seq[String] = classPathDirs.filter(p => p.substring(p.lastIndexOf(File.separatorChar) + 1) == "test-classes")

  // TODO [test] resources

  /**
   * /classes directories
   */
  lazy val mainClassPaths: Seq[String] = classPathDirs.filter(p => p.substring(p.lastIndexOf(File.separatorChar) + 1) == "classes")

  /**
   * /scoverage-classes directories
   */
  lazy val scoverageClassPaths: Seq[String] = classPathDirs.filter(p => p.substring(p.lastIndexOf(File.separatorChar) + 1) == "scoverage-classes")

  /**
   * This value signifies the main classpath to use for a connect server, note it will be filtered out of the config
   * map that is generally applied and used to set the classpath for the spawned jvm.
   */
  val MAIN_CLASSPATH: String = "MAIN_CLASSPATH"

  /**
   * Use this configuration to add your non-test classes to connect servers, for example custom query plans or
   * expressions
   */
  lazy val mainClassPathsConfig: (String, String) = MAIN_CLASSPATH -> mainClassPaths.mkString(File.pathSeparatorChar.toString)

  /**
   * Use this configuration to add scoverage instrumented main classes, falling back to main classes, to connect servers, for example custom query plans or
   * expressions
   */
  lazy val scoverageClassPathsConfig: (String, String) = MAIN_CLASSPATH -> (scoverageClassPaths ++ mainClassPaths).mkString(File.pathSeparatorChar.toString)

  /**
   * Use this configuration to add your test classes to connect servers, for example custom query plans or
   * expressions in your test classes
   */
  lazy val testClassesPathsConfig: (String, String) = MAIN_CLASSPATH -> testClassPaths.mkString(File.pathSeparatorChar.toString)

  /**
   * Use this configuration to add both your test and main classes to connect servers, for example custom query plans or
   * expressions in your test classes
   */
  lazy val fullClassPathConfig: (String, String) = MAIN_CLASSPATH -> (mainClassPaths ++ testClassPaths).mkString(File.pathSeparatorChar.toString)

  val DEBUG_CONNECT_LOGS_SYS = "spark.debug.sc.jvm.client"

  /**
   * Enables debug logging on the spawned connect server, use this when your tests hang or otherwise unexpectedly quit
   */
  val useDebugConnectLogs: (String, String) = DEBUG_CONNECT_LOGS_SYS -> "true"



  /**
   * PREFIX configuration item keys with this in order to provide additional JVM options (e.g. -D's or memory settings)
   */
  val FLAT_JVM_OPTION: String = "SPAWNED_JVM_OPT_"

  /**
   * Use with '4g', for example to spawn connect servers of sufficient memory
   * @param of
   * @return
   */
  def connectMemory(of: String): (String, String) = jvmOpt(("MEMORY" -> ("-Xmx"+of)))

  /**
   * provide additional jvm options, the name is used to provide a key and is not used on the command line, only the value
   * @param pair
   * @return
   */
  def jvmOpt(pair: (String, String)) = FLAT_JVM_OPTION+pair._1 -> pair._2

  private val _runtimeConnectClientConfig = new AtomicReference[Map[String,String]](configFromPrefix("SPARKUTILS_CONNECT_CLIENT."))

  def setRuntimeConnectClientConfig(config: Map[String, String]): Unit = {
    _runtimeConnectClientConfig.set(config)
  }

  /**
   * Used by ConnectUtils.connectSparkSession, be sure to call setRuntimeConnectClientConfig before running tests
   */
  lazy val runtimeConnectClientConfig: Map[String, String] = _runtimeConnectClientConfig.get()


  private val _runtimeClassicConfig = new AtomicReference[Map[String,String]](configFromPrefix("SPARKUTILS_CONNECT_SERVER."))

  def setRuntimeClassicConfig(config: Map[String, String]): Unit = {
    _runtimeClassicConfig.set(config)
  }

  /**
   * Used by ClassicUtils.classicSparkSession, be sure to call setRuntimeClassicConfig before running tests
   */
  lazy val runtimeClassicConfig: Map[String, String] = _runtimeClassicConfig.get()

  protected val tpath = new AtomicReference[String](
    stringEnvOrProp("SPARKUTILS_TEST_OUTPUTDIR", "./target/testData")
  )

  def ouputDir = tpath.get


  def setPath(newPath: String) = {
    tpath.set(newPath)
  }

  def path(suffix: String) = s"${tpath.get}/$suffix"

}
