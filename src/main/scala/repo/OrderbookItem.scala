package com.guardian
package repo

import AppError.OrderbookUpdateError
import entity._

import cats.implicits.catsSyntaxEitherId

private[repo] case class OrderbookItem(
    seq: Long,
    maxLevel: Int,
    bids: Seq[Option[(Price, Qty, Nano)]],
    asks: Seq[Option[(Price, Qty, Nano)]],
    marketTs: Nano,
    bananaTs: Micro
) {

  def insert(price: Price, qty: Qty, sourceTs: Nano, level: Int, side: Side): Seq[Option[(Price, Qty, Nano)]] = {
    val index = level - 1
    val current = side.value.toChar match {
      case 'B' => bids
      case _   => asks
    }
    val (front, back) = current.splitAt(index)
    val res           = front ++ Vector(Some(price, qty, sourceTs)) ++ back
    res.dropRight(res.size - maxLevel)
  }

  def delete(side: Side, level: Int, numDeletes: Int): Seq[Option[(Price, Qty, Nano)]] = {
    val index = level - 1
    val current = side.value.toChar match {
      case 'B' => bids
      case _   => asks
    }
    val (first, rest) = current.splitAt(index)
    val (_, last)     = rest.splitAt(numDeletes)
    val res           = first ++ last
    res.dropRight(res.size - maxLevel)
  }

  def update(
      side: Side,
      level: Int,
      price: Price,
      qty: Qty,
      marketTs: Nano
  ): Either[AppError, Seq[Option[(Price, Qty, Nano)]]] = {
    val index = level - 1
    val current = side.value.toChar match {
      case 'B' => bids
      case _   => asks
    }
    if (index > current.size - 1) {
      OrderbookUpdateError(level, current.size).asLeft
    }
    else {
      current.updated(index, Some(price, qty, marketTs)).asRight
    }
  }
}

object OrderbookItem {
  def reconstruct(
      list: Seq[Option[(Price, Qty, Nano)]],
      side: Byte,
      seq: Long,
      maxLevel: Int,
      marketTs: Nano,
      bananaTs: Micro,
      origin: OrderbookItem
  ): OrderbookItem = {
    val temp = side.toChar match {
      case 'B' => origin.copy(bids = list)
      case _   => origin.copy(asks = list)
    }
    temp.copy(seq = seq, maxLevel = maxLevel, marketTs = marketTs, bananaTs = bananaTs)
  }

  def empty(maxLevel: Int): OrderbookItem =
    OrderbookItem(
      seq = -1,
      maxLevel = maxLevel,
      bids = Vector.empty,
      asks = Vector.empty,
      marketTs = Nano("0"),
      bananaTs = Micro(0L)
    )
}
