package com.sparkutils.testing.markers

/**
 * Simple marker trait to indicate a test suite is safe for use in a pure connect setup
 */
trait ConnectSafe

object ConnectSafe {
  /**
   * Checks if the ConnectSafe trait is present, indicating a test suite is safe for use in a pure connect setup
   */
  def isConnectSafe(clazz: Class[_]): Boolean =
    classOf[ConnectSafe].isAssignableFrom(clazz)
}
