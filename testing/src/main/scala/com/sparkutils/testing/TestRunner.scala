package com.sparkutils.testing

import com.google.common.reflect.ClassPath
import com.sparkutils.testing.SparkTestUtils.disableClassicTesting
import com.sparkutils.testing.markers.{ConnectSafe, DontRunOnPureConnect}
import org.scalatest.Suite

import java.io.IOException
import java.util

/**
 * Extend this trait as an object to create a test runner
 */
trait TestRunner {

  /**
   * Search this package name to find tests
   */
  val packageName: String

  /**
   * ClassLoader used to find tests by default uses this classloader
   */
  val classLoader: ClassLoader = this.getClass.getClassLoader

  /**
   * Project name to use for stdout logging
   */
  val projectName: String

  /**
   * Provides -oWDFT by default
   */
  val scalaTestArgs: Seq[String] = Seq("-oWDFT")

  def numberOfBatches(numberOfClasses: Int) = numberOfClasses / 2 / 10
  val argsPerBatch = 20

  @throws[IOException]
  def testViaClassLoader(batchStartingNumber: Int): Unit = {
    testViaClassLoader(Array.empty[String], batchStartingNumber)
  }

  @throws[IOException]
  def testViaClassLoader(args: Array[String]): Unit = {
    testViaClassLoader(args, 0)
  }

  /**
   * By default, checks if SPARKUTILS_DISABLE_CLASSIC_TESTS is true and, if so, only selects suites implementing the
   * ConnectSafe trait, otherwise it allows the suite through.
   * Override to add different filters for your suite class files
   * @param clazz
   * @return
   */
  def usableTestSuite(clazz: Class[_]): Boolean =
    if (disableClassicTesting)
      ConnectSafe.isConnectSafe(clazz) && !DontRunOnPureConnect.shouldNotRunOnPureConnect(clazz)
    else
      true

  @throws[IOException]
  def testViaClassLoader(args: Array[String], batchStartingNumber: Int): Unit = {
    val oargs = new util.ArrayList[String]
    scalaTestArgs.foreach(oargs.add)

    for (i <- 0 until args.length) {
      oargs.add(args(i))
    }
    val classargs = new util.ArrayList[String]
    val classPath = ClassPath.from(classLoader)
    val framelessInfo = classPath.getTopLevelClassesRecursive(packageName)
    val itr = framelessInfo.iterator()
    while(itr.hasNext) {
      try {
        val clazz = itr.next().load()
        val suite = clazz.newInstance.asInstanceOf[Suite]
        // it's a suite
        if (usableTestSuite(clazz)) {
          // we should use it
          classargs.add("-s")
          classargs.add(clazz.getName)
        }
      } catch {
        case t: Throwable =>
        // ignore
      }
    }
    val numberOfBatchesN = numberOfBatches(classargs.size)

    val classargItr = classargs.iterator
    var j = 0
    while (j < (batchStartingNumber * argsPerBatch) && classargItr.hasNext) {
      classargItr.next

      j += 1
    }
    for (i <- batchStartingNumber until numberOfBatchesN) {
      System.out.println(s"$projectName - starting test batch $i")
      val joined = new Array[String](oargs.size + argsPerBatch)
      oargs.toArray[String](joined)
      var j = 0
      while (j < argsPerBatch && classargItr.hasNext) {
        joined(oargs.size + j) = classargItr.next

        j += 1
      }
      org.scalatest.tools.Runner.run(joined)
      System.out.println(s"$projectName - gc'ing after finishing test batch $i")
      System.gc()
      System.gc()
    }
    System.out.println(s"all $projectName test batches completed")
  }

  /**
   * Run against any test found in the current classloader
   *
   * @param args
   */
  def scalaTestRunner(args: Array[String]): Unit = {
    org.scalatest.tools.Runner.run(args)
  }

  @throws[IOException]
  def test(batchStartingNumber: Int): Unit = {
    test(Array.empty[String], batchStartingNumber)
  }

  @throws[IOException]
  def test(args: Array[String]): Unit = {
    if (args.size == 1 && args(0).startsWith("just="))
      runTestName(args(0).drop("just=".length))
    else
      test(args, 0)
  }

  @throws[IOException]
  def test(args: Array[String], batchStartingNumber: Int): Unit = {
    testViaClassLoader(args, batchStartingNumber)
  }

  @throws[IOException]
  def runTestName(testName: String): Unit = {
    val oargs = new util.ArrayList[String]
    scalaTestArgs.foreach(oargs.add)
    oargs.add("-s")
    oargs.add(testName)
    val joined = new Array[String](oargs.size)
    oargs.toArray[String](joined)
    org.scalatest.tools.Runner.run(joined)
    System.out.println(s"$projectName - $testName finished properly")
  }

  @throws[IOException]
  def test(): Unit = {
    test(Array.empty[String])
  }

}
