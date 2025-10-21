package com.sparkutils.testing

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
}
