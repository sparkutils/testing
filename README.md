# testing

A collection of utility functions and base traits to aid testing on Spark.

Key features:

* Simple base trait for session handling and Windows compat
* Series of wrapper functions to handle Databricks / Fabric, Version specific handling, codegen and more
* Test runners for server side execution, with simple progress output on Fabric and Databricks (assuming proper shading)
  * tests are run in batches, and can be restarted from a specific batch
  * individual tests can be re-run
  * configuration can be programmatic or driven by environment variables
* SparkSession handling is abstracted allowing for easy sharing

## Spark 4 Specific handling

* ScalaTests are run against both Spark Connect and Classic
* Tests can run remotely against Connect Severs (including Databricks 17.3)
  - local class and test files will be automatically sent to the servers (jars can additionally be sent via config) enabling map and udf usage 
* Session handling works with existing connections,
  - automatically disabling rule Suites that are assignable to DontRunOnPureConnect and running those with ConnectSafe marker traits.
  - will start up local Connect Servers for you in the IDE

## Usage

Implement TestRunner for your test classes, it can be an application that simply calls test(args) running all Suite's in your packageName.  Users can pass in arguments to Scalatest but also supply "just=full.qualified.test.suite.name" to start a single test e.g.:

```scala
package com.sparkutils.qualityTests

import com.sparkutils.testing.TestRunner
import com.sparkutils.testing.TestUtilsEnvironment.setupDefaultsViaCurrentSession

object QualityTestRunner extends TestRunner {

  val packageName: String = "com.sparkutils"

  val projectName: String = "Quality"

  override val classLoader: ClassLoader = classOf[RemoteFunctionTests].getClassLoader

  // when on Fabric or Databricks disables cluster tests
  setupDefaultsViaCurrentSession()

  def main(args: Array[String]): Unit = test(args)
}
```

in notebooks the following setup is typical (using the QualityTestRunner example):

```scala
import com.sparkutils.qualityTests.QualityTestRunner
import com.sparkutils.testing.SparkTestUtils

val accountKey = dbutils.secrets.get("AKV", storageVaultKey) 
val keyMap = Map(s"fs.azure.account.key.XXX" -> accountKey)
SparkTestUtils.setRuntimeConnectClientConfig(keyMap)
SparkTestUtils.setRuntimeClassicConfig(keyMap)

// when init script is available comment the below to test connect usage
//System.setProperty("SPARKUTILS_DISABLE_CONNECT_TESTS","true")

// when init script is available _and_ you have a UC shared cluster, uncomment the below to force connect usage only
System.setProperty("SPARKUTILS_DISABLE_CLASSIC_TESTS","true")

val root_path = loc
SparkTestUtils.setPath(root_path+"/qualityTests")
QualityTestRunner.test()
```

which will start the batch running of tests.

### Defining Tests

```scala
trait SharedPureConnectTests extends FunSuite with SharedSessions with SparkTestSuite with ConnectSafe {

  override def sparkConnectServerConfig(): Map[String, String] =
    super.sparkConnectServerConfig() + // useDebugConnectLogs +
      scoverageClassPathsConfig + connectMemory("4g") +
      (("spark.sql.extensions", "com.sparkutils.quality.impl.extension.QualitySparkExtension"))

}
```

This defines parameters for using scoverage, to use 4g on the spawned server and to use an extension, then simply use normal Scalatest tests:

```scala
class MyTestSuite extends SharedPureConnectTests {
  test("my logic") {
    sparkSession.sql(s"select ...")....
  }
}

```

The 'my logic' test will run on both Connect and Classic for Spark 4 builds.
