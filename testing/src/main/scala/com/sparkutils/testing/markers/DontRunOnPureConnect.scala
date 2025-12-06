package com.sparkutils.testing.markers

/**
 * Simple marker trait to indicate a test suite should not be run when only pure connect is available
 * (e.g. classic tests that you also want to have run with local connect, but never remote or on Shared Compute).
 */
trait DontRunOnPureConnect

object DontRunOnPureConnect {
  /**
   * Checks if the ConnectSafe trait is present, indicating a test suite is safe for use in a pure connect setup
   */
  def shouldNotRunOnPureConnect(clazz: Class[_]): Boolean =
    classOf[DontRunOnPureConnect].isAssignableFrom(clazz)
}

