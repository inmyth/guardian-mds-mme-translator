package com.guardian
package repo

import Fixtures._
import entity.{Micro, Price, Qty}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class OrderbookItemSpec extends AsyncWordSpec with Matchers {
  import OrderbookItemSpec._

  "insert" should {
    "return new list with new element" in {
      val newPrice = Price(107)
      val newQty   = Qty(400L)
      val serverTs = Micro(1675752890L)
      val levels   = List(1, maxLevel / 2, maxLevel)
      val expecteds = levels.map(level => {
        val (a, b) = bids.splitAt(level - 1)
        (a ++ Vector(Some(newPrice, newQty, serverTs)) ++ b).dropRight(1)
      })
      val actuals = levels.map(level => orderbookItem.insert(newPrice, newQty, serverTs, level, side))
      actuals shouldBe expecteds
    }
  }
  "delete" should {
    "return new list with area of deletion replaced with none" when {
      "level is the first" in {
        val level      = 1
        val numDeletes = 3
        val expected   = bids.updated(0, None).updated(1, None).updated(2, None).filter(_.isDefined)
        val actual     = orderbookItem.delete(side, level, numDeletes)
        actual shouldBe expected
      }
      "level is middle" in {
        val level      = 5
        val numDeletes = 3
        val expected   = bids.updated(4, None).updated(5, None).updated(6, None).filter(_.isDefined)
        val actual     = orderbookItem.delete(side, level, numDeletes)
        actual shouldBe expected
      }
      "level is last" in {
        val level      = 10
        val numDeletes = 1
        val expected   = bids.updated(9, None).filter(_.isDefined)
        val actual     = orderbookItem.delete(side, level, numDeletes)
        actual shouldBe expected
      }
      "level is middle and numDeletes > size" in {
        val level      = 8
        val numDeletes = 5
        val expected   = bids.updated(7, None).updated(8, None).updated(9, None).filter(_.isDefined)
        val actual     = orderbookItem.delete(side, level, numDeletes)
        actual shouldBe expected
      }
    }
    "update" should {
      "return updated list" in {
        val level    = 2
        val newPrice = Price(400)
        val newQty   = Qty(1500L)
        val serverTs = Micro(1675752890L)
        val actual   = orderbookItem.update(side, level, newPrice, newQty, serverTs)
        val expected = bids.updated(1, Some((newPrice, newQty, serverTs)))
        actual shouldBe expected
      }
    }
  }
}

object OrderbookItemSpec {

  val orderbookItem: OrderbookItem = OrderbookItem(
    seq = seq,
    maxLevel = maxLevel,
    bids = bids,
    asks = asks,
    marketTs = Micro(0L),
    bananaTs = Micro(0L)
  )
}
