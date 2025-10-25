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
      .filterNot { e: String =>
        val fileName = e.substring(e.lastIndexOf(File.separatorChar) + 1)
        fileName.endsWith(".jar") && (new File(fileName).isDirectory)
      }

  /**
   * All jars on the classpath, these are assumed to be safe to use for connect servers
   */
  lazy val classPathJars: Seq[String] =
    System
      .getProperty("java.class.path")
      .split(File.pathSeparatorChar)
      .filter { e: String =>
        val fileName = e.substring(e.lastIndexOf(File.separatorChar) + 1)
        fileName.endsWith(".jar")
      }

  /**
   * Removes connect client jars to allow the correct runtime class to be chosen
   */
  lazy val connectServerJars: Seq[String] =
    classPathJars.filterNot(_.contains("spark-connect-client-jvm"))

  /**
   * /test-classes directories
   */
  lazy val testClassPaths: Seq[String] = classPathDirs.filter(_.endsWith("test-classes"))

  // TODO [test] resources

  /**
   * /classes directories
   */
  lazy val mainClassPaths: Seq[String] = classPathDirs.filterNot(_.endsWith("classes"))

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
}
