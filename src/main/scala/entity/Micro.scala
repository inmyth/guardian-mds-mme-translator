package com.guardian
package entity

case class Micro(value: Long) extends AnyVal

object Micro {

  def fromSecondAndMicro(unixSec: Int, nanos: Int): Micro =
    Micro(s"$unixSec$nanos".substring(0, 16).toLong)
}
