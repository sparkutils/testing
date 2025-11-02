package com.sparkutils.testing

import java.io.File
import scala.util.Try

object Utils {

  def booleanEnv(env: String): Boolean = {
    val tmp = System.getenv(env)
    if (tmp eq null)
      false
    else
      Try {
        tmp.toBoolean
      }.getOrElse(false)
  }

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
}
