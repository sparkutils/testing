# testing

A collection of utility functions and base traits to aid testing on Spark.

Key features:

* Simple base trait for session handling and Windows compat
* Series of wrapper functions to handle Databricks / Fabric, Version specific handling, codegen and more
* Test runners for server side execution, with simple progress output on Fabric and Databricks (assuming proper shading)
** tests are run in batches, and can be restarted from a specific batch
** individual tests can be re-run

## Spark 4 Specific handling

* ScalaTests are run against both Spark Connect and Classic
* ClassicOnly tags are only run on Classic using the TestUtils withFixture handling
