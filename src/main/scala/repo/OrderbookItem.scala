package com.guardian
package repo

import entity.{Micro, Price, Qty, Side}

import cats.implicits.catsSyntaxEitherId
import com.guardian.AppError.OrderbookUpdateError

private[repo] case class OrderbookItem(
    seq: Long,
    maxLevel: Int,
    bids: Seq[Option[(Price, Qty, Micro)]],
    asks: Seq[Option[(Price, Qty, Micro)]],
    marketTs: Micro,
    bananaTs: Micro
) {

  def insert(price: Price, qty: Qty, sourceTs: Micro, level: Int, side: Side): Seq[Option[(Price, Qty, Micro)]] = {
    val index = level - 1
    val current = side.value.toChar match {
      case 'B' => bids
      case _   => asks
    }
    val (front, back) = current.splitAt(index)
    val res           = front ++ Vector(Some(price, qty, sourceTs)) ++ back
    res.dropRight(res.size - maxLevel)
  }

  def delete(side: Side, level: Int, numDeletes: Int): Seq[Option[(Price, Qty, Micro)]] = {
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
      marketTs: Micro
  ): Either[AppError, Seq[Option[(Price, Qty, Micro)]]] = {
    val index = level - 1
    val current = side.value.toChar match {
      case 'B' => bids
      case _   => asks
    }
    if (index >= current.size) {
      OrderbookUpdateError(level, current.size).asLeft
    }
    else {
      current.updated(index, Some(price, qty, marketTs)).asRight
    }
  }
}

object OrderbookItem {
  def reconstruct(
      list: Seq[Option[(Price, Qty, Micro)]],
      side: Byte,
      seq: Long,
      maxLevel: Int,
      marketTs: Micro,
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
      marketTs = Micro(0L),
      bananaTs = Micro(0L)
    )
}
