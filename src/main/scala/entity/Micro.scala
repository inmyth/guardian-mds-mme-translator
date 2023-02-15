package com.guardian
package entity

case class Micro(value: Long) extends AnyVal

object Micro {

  def fromSecondAndMicro(unixSec: Int, nanos: Int): Micro = {
    val ns = nanos.toString.padTo(6, '0')
    Micro(s"$unixSec$ns".substring(0, 16).toLong)
  }
}
