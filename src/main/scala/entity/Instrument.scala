package com.guardian
package entity

case class Instrument (value: String) extends AnyVal

object Instrument {
  def apply(v: Array[Byte]): Instrument = new Instrument(new String(v))
  def apply(v: String): Instrument = new Instrument(v)
}