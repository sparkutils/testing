package com.sparkutils.testing

import org.scalatest.Tag

/**
 * Annotate your scalatest tests with NoSparkConnect to disable running on Spark Connect
 */
object NoSparkConnect extends Tag("com.sparkutils.testing.NoSparkConnect")
