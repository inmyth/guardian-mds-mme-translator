package com.guardian
package entity

case class Side(value: Byte) extends AnyVal {

  override def toString: String = value.asInstanceOf[Char].toString
}