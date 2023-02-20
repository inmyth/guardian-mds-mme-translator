package com.guardian

import entity._

object Fixtures {

  val bidPrice1: Price  = Price(100)
  val bidPrice2: Price  = Price(99)
  val bidPrice3: Price  = Price(98)
  val bidPrice4: Price  = Price(97)
  val bidPrice5: Price  = Price(96)
  val bidPrice6: Price  = Price(95)
  val bidPrice7: Price  = Price(94)
  val bidPrice8: Price  = Price(93)
  val bidPrice9: Price  = Price(92)
  val bidPrice10: Price = Price(91)
  val bidQty1: Qty      = Qty(1000L)
  val bidQty2: Qty      = Qty(950L)
  val bidQty3: Qty      = Qty(900L)
  val bidQty4: Qty      = Qty(850L)
  val bidQty5: Qty      = Qty(800L)
  val bidQty6: Qty      = Qty(750L)
  val bidQty7: Qty      = Qty(700L)
  val bidQty8: Qty      = Qty(650L)
  val bidQty9: Qty      = Qty(600L)
  val bidQty10: Qty     = Qty(550L)
  val askTime1: Micro   = Micro(1675749643)
  val askTime2: Micro   = Micro(1675749645)
  val askTime3: Micro   = Micro(1675749647)
  val askTime4: Micro   = Micro(1675749649)
  val askTime5: Micro   = Micro(1675749651)
  val askTime6: Micro   = Micro(1675749653)
  val askTime7: Micro   = Micro(1675749655)
  val askTime8: Micro   = Micro(1675749657)
  val askTime9: Micro   = Micro(1675749659)
  val askTime10: Micro  = Micro(1675749661)

  val askPrice1: Price  = Price(102)
  val askPrice2: Price  = Price(103)
  val askPrice3: Price  = Price(104)
  val askPrice4: Price  = Price(105)
  val askPrice5: Price  = Price(106)
  val askPrice6: Price  = Price(107)
  val askPrice7: Price  = Price(108)
  val askPrice8: Price  = Price(109)
  val askPrice9: Price  = Price(110)
  val askPrice10: Price = Price(111)
  val askQty1: Qty      = Qty(1300L)
  val askQty2: Qty      = Qty(1350L)
  val askQty3: Qty      = Qty(1400L)
  val askQty4: Qty      = Qty(1450L)
  val askQty5: Qty      = Qty(1500L)
  val askQty6: Qty      = Qty(1550L)
  val askQty7: Qty      = Qty(1600L)
  val askQty8: Qty      = Qty(1650L)
  val askQty9: Qty      = Qty(1700L)
  val askQty10: Qty     = Qty(1750L)
  val bidTime1: Micro   = Micro(1675751686)
  val bidTime2: Micro   = Micro(1675751688)
  val bidTime3: Micro   = Micro(1675751690)
  val bidTime4: Micro   = Micro(1675751692)
  val bidTime5: Micro   = Micro(1675751694)
  val bidTime6: Micro   = Micro(1675751696)
  val bidTime7: Micro   = Micro(1675751698)
  val bidTime8: Micro   = Micro(1675751700)
  val bidTime9: Micro   = Micro(1675751702)
  val bidTime10: Micro  = Micro(1675751704)

  val bids: Seq[Some[(Price, Qty, Micro)]] = Vector(
    Some(bidPrice1, bidQty1, bidTime1),
    Some(bidPrice2, bidQty2, bidTime2),
    Some(bidPrice3, bidQty3, bidTime3),
    Some(bidPrice4, bidQty4, bidTime4),
    Some(bidPrice5, bidQty5, bidTime5),
    Some(bidPrice6, bidQty6, bidTime6),
    Some(bidPrice7, bidQty7, bidTime7),
    Some(bidPrice8, bidQty8, bidTime8),
    Some(bidPrice9, bidQty9, bidTime9),
    Some(bidPrice10, bidQty10, bidTime10)
  )

  val asks: Seq[Some[(Price, Qty, Micro)]] = Vector(
    Some(askPrice1, askQty1, askTime1),
    Some(askPrice2, askQty2, askTime2),
    Some(askPrice3, askQty3, askTime3),
    Some(askPrice4, askQty4, askTime4),
    Some(askPrice5, askQty5, askTime5),
    Some(askPrice6, askQty6, askTime6),
    Some(askPrice7, askQty7, askTime7),
    Some(askPrice8, askQty8, askTime8),
    Some(askPrice9, askQty9, askTime9),
    Some(askPrice10, askQty10, askTime10)
  )

  val channel: Config.Channel = Config.Channel.eq
  val side: Side              = Side('B'.asInstanceOf[Byte])
  val maxLevel: Byte          = 10
  val oid: OrderbookId        = OrderbookId(37)
  val symbol: Instrument      = Instrument("PTT")
  val seq                     = 677L
  val marketTs: Micro         = Micro(1675769709L)
  val bananaTs: Micro         = Micro(1675769709L)
}
