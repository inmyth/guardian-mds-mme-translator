package com.guardian
package entity

case class Nano(value: String) extends AnyVal

object Nano {
  def fromSecondAndNano(unixSec: Int, nanos: Int): Nano = {
    Nano(s"$unixSec$nanos")
  }
}