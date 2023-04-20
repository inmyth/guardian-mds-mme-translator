package com.guardian

import entity._

object Fixtures {

  val bidPrice1: Price  = Price(10000)
  val bidPrice2: Price  = Price(9900)
  val bidPrice3: Price  = Price(9800)
  val bidPrice4: Price  = Price(9700)
  val bidPrice5: Price  = Price(9600)
  val bidPrice6: Price  = Price(9500)
  val bidPrice7: Price  = Price(9400)
  val bidPrice8: Price  = Price(9300)
  val bidPrice9: Price  = Price(9200)
  val bidPrice10: Price = Price(9100)
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
  val askTime1: Micro   = Micro(1675749643_123456L)
  val askTime2: Micro   = Micro(1675749645_123456L)
  val askTime3: Micro   = Micro(1675749647_123456L)
  val askTime4: Micro   = Micro(1675749649_123456L)
  val askTime5: Micro   = Micro(1675749651_123456L)
  val askTime6: Micro   = Micro(1675749653_123456L)
  val askTime7: Micro   = Micro(1675749655_123456L)
  val askTime8: Micro   = Micro(1675749657_123456L)
  val askTime9: Micro   = Micro(1675749659_123456L)
  val askTime10: Micro  = Micro(1675749661_123456L)

  val askPrice1: Price  = Price(10200)
  val askPrice2: Price  = Price(10300)
  val askPrice3: Price  = Price(10400)
  val askPrice4: Price  = Price(10500)
  val askPrice5: Price  = Price(10600)
  val askPrice6: Price  = Price(10700)
  val askPrice7: Price  = Price(10800)
  val askPrice8: Price  = Price(10900)
  val askPrice9: Price  = Price(11000)
  val askPrice10: Price = Price(11100)
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
  val bidTime1: Micro   = Micro(1675751686_123456L)
  val bidTime2: Micro   = Micro(1675751688_123456L)
  val bidTime3: Micro   = Micro(1675751690_123456L)
  val bidTime4: Micro   = Micro(1675751692_123456L)
  val bidTime5: Micro   = Micro(1675751694_123456L)
  val bidTime6: Micro   = Micro(1675751696_123456L)
  val bidTime7: Micro   = Micro(1675751698_123456L)
  val bidTime8: Micro   = Micro(1675751700_123456L)
  val bidTime9: Micro   = Micro(1675751702_123456L)
  val bidTime10: Micro  = Micro(1675751704_123456L)

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

  val channel: Config.Channel    = Config.Channel.eq
  val side: Side                 = Side('B'.asInstanceOf[Byte])
  val maxLevel: Byte             = 10
  val oid: OrderbookId           = OrderbookId(37)
  val symbol: Instrument         = Instrument("PTT")
  val seq                        = 677L
  val second: Int                = 1677238290
  val dealDateTime: Long         = 1675763719L
  val tradeTs: Long              = 1675763719123L
  val marketTs: Micro            = Micro(1675735830_123456L) // Tuesday, February 7, 2023 9:10:30 AM
  val bananaTs: Micro            = Micro(1675735840_123456L)
  val change                     = 100000L
  val changePercent              = 33
  val secType                    = "equity"
  val secDesc                    = "SecurityLongName"
  val allowShortSell: Byte       = 'Y'
  val allowNVDR: Byte            = 'A'
  val allowShortSellOnNVDR: Byte = 'Y'
  val allowTTF: Byte             = 'Y'
  val isValidForTrading: Byte    = 'A'
  val lotRoundSize: Int          = 1
  val parValue                   = 23099440L
  val sectorNumber               = "SET"
  val underlyingSecCode          = 123
  val underlyingSecName          = "UDR"
  val maturityDate               = 20220314
  val contractMultiplier         = 3
  val settlMethod                = "NA"
  val settlPrice: Price          = Price(1200)
  val decimalsInPrice: Short  = 2

  val t1225: Micro = Micro(1675747530_123456L)
  val t1235: Micro = Micro(1675748130_123456L)
  val t1625: Micro = Micro(1675761930_123456L)
  val t1635: Micro = Micro(1675762530_123456L)
  val t1840: Micro = Micro(1675770006_123456L)
  val t1850: Micro = Micro(1675770606_123456L)
  val t2325: Micro = Micro(1675787106_123456L)
  val t2335: Micro = Micro(1675787706_123456L)

  val openPrice1: Price = Price(10000)
  val closePrice1: Price = Price(11000)
  val openPrice2: Price = Price(12000)
  val closePrice2: Price = Price(13000)
  val nightOpenPrice: Price = Price(14000)
  val nightClosePrice: Price = Price(15000)
  val openPriceA: Price8 = Price8(10000L)
  val openPriceB: Price8 = Price8(12000L)
  val closePriceA: Price8 = Price8(8000L)
  val closePriceB: Price8 = Price8(9000L)
  val highPriceA: Price8 = Price8(19000L)
  val highPriceB: Price8 = Price8(21000L)
  val lowPriceA: Price8 = Price8(7000L)
  val lowPriceB: Price8 = Price8(6000L)
  val prevCloseA: Price8 = Price8(7500L)
  val prevCloseB: Price8 = Price8(8500L)
  val tradedVolA: Qty = Qty(56000000L)
  val tradedVolB: Qty = Qty(6670000L)
  val tradedValA: Price8 = Price8(6845460000L)
  val tradedValB: Price8 = Price8(973330000L)
  val changeA: Price8 = Price8(3005656L)
  val changeB: Price8 = Price8(6780000L)

  val action: FlatPriceLevelAction = FlatPriceLevelAction(
    oid = oid,
    symbol = symbol,
    marketTs = askTime1,
    bananaTs = bananaTs,
    maxLevel = 10,
    price = askPrice1,
    qty = askQty1,
    level = 1,
    side = Side('A'),
    levelUpdateAction = 'N',
    numDeletes = 0
  )
}
