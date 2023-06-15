package com.guardian
package entity

case class Nano(value: String) extends AnyVal

object Nano {
  def fromSecondAndNano(unixSec: Int, nanos: Int): Nano = {
    val ns = "%09d".format(nanos)
    Nano(s"$unixSec$ns")
  }
}