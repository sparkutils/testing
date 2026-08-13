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
  - SPARK_REMOTE is detected to disable classic tests by default 
* Session handling works with existing connections,
  - with Connect connections, automatically disabling rule Suites that are assignable to DontRunOnPureConnect and running those with ConnectSafe marker traits.
  - will start up local Connect Servers for you in the IDE

## Dependencies

```xml
<dependency>
  <groupId>com.sparkutils.</groupId>
  <artifactId>testing_${dbrCompatVersion}${sparkCompatVersion}_${scalaCompatVersion}</artifactId>
  <version>${testingVersion}</version>
  <scope>test</scope>
</dependency>
```

Current supported runtime versions:

| Spark Version | sparkShortVersion | OSS/Test dbrCompatVersion | Databricks dbrCompatVersion    | scalaCompatVersion   |
|---------------|-------------------|---------------------------|--------------------------------|----------------------|
| 3.0           | 3.0               | 3.0.0.oss_                |                                | 2.12                 | 
| 3.1.3         | 3.1               | 3.1.3.oss_                |                                | 2.12                 | 
| 3.2           | 3.2               | 3.2.0.oss_                |                                | 2.12                 | 
| 3.2.1         | 3.2               | 3.2.1.oss_                |                                | 2.12                 | 
| 3.3.2         | 3.3               | 3.3.2.oss_                |                                | 2.12                 | 
| 3.3.2         | 3.3               | 3.3.2.oss_                |                                | 2.12                 | 
| 3.4.1         | 3.4               | 3.4.1.oss_                | 13.3.dbr_                      | 2.12                 | 
| 3.5.0         | 3.5               | 3.5.0.oss_                | 14.3.dbr_                      | 2.12                 | 
| 3.5.0         | 3.5               | 3.5.0.oss_                | 15.4.dbr_                      | 2.12                 |
| 3.5.0         | 3.5               | 3.5.0.oss_                | 16.4.dbr_                      | 2.12                 |
| 4.0.0         | 4.0               | 4.0.0.oss_                | 17.3.dbr_                      | 2.13                 |
| 4.1.0         | 4.1               | 4.1.0.oss_                | 18.3.dbr_                      | 2.13                 |
| 4.2.0         | 4.2               | 4.2.0.oss_                | 18.3.dbr_  (for use on DBR 19) | 2.13                 |

Spark runtimes 3.0,3.1.3, 3.2.0, 3.2.1, 3.3.2 and 3.4.x are deprecated as are DBR's 12.2 and 13.3 and will be removed as the next testing release.

When building on OSS and deploying shaded tests on Databricks the following approach is needed:

```xml
<dependency>
  <groupId>com.sparkutils.</groupId>
  <artifactId>testing_${ossTestCompatVersion}${sparkCompatVersion}_${scalaCompatVersion}</artifactId>
  <version>${testingVersion}</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>com.sparkutils</groupId>
  <artifactId>testing_${dbrCompatVersion}${sparkCompatVersion}_${scalaCompatVersion}</artifactId>
  <version>${testingVersion}</version>
  <scope>compile</scope>
</dependency>
```

### Managing runtime behaviour based on Spark environment

```xml
<dependency>
  <groupId>com.sparkutils</groupId>
  <artifactId>testing-runtime_${dbrCompatVersion}${sparkCompatVersion}_${scalaCompatVersion}</artifactId>
  <version>${testingVersion}</version>
</dependency>
```

The testing-runtime dependency is aimed at libraries and applications that must have version specific behaviour or knowledge
of the runtime environment or being tested to correctly function.  It is not recommended for most projects, 
those making use of both Connect and Classic functionality (e.g. SparkSessionExtensions or Expressions) may benefit from
ConnectWhenForced (as with Quality and dmn-4-spark). 

The library provides a number of Spark version detection functionality and setting of output directories, testing environment etc.  
It is intended to be a compile time dependency to allow code to react to test cases or Spark environments differently,
for example the use of ConnectWhenForced.someOrForcedConnect allows code to force Spark Connect style behaviour.

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

  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

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

The 'my logic' test will run on both Connect and Classic for Spark 4 builds, or just ConnectSafe tests when defining SPARK_REMOTE for example.
