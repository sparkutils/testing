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
* Session handling works with existing connections,
  - automatically disabling rule Suites that are assignable to DontRunOnPureConnect and running those with ConnectSafe marker traits.
  - will start up local Connect Servers for you in the IDE
