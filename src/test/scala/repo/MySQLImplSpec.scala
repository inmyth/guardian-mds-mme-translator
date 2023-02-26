package com.guardian
package repo

import Config.{Channel, MySqlConfig}
import Fixtures._
import entity.{Micro, Qty, Side}

import monix.execution.Scheduler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class MySQLImplSpec extends AsyncWordSpec with Matchers {
  import MySQLImplSpec._
  implicit val ec: Scheduler = monix.execution.Scheduler.Implicits.global

  "MqSQLImpl" when {
    "either" should {
      "drop and recreate the db" in {
        (for {
          _ <- storeEq.createTables
        } yield ()).runToFuture.map(_ shouldBe ())
      }
    }
    "eq" when {
      val store = storeEq
      "saveTradableInstrument, getInstrument" should {
        "save the instrument along with other data and get it back" in {
          (for {
            _ <- store.saveTradableInstrument(
              oid = oid,
              symbol = symbol,
              secType = secType,
              secDesc = secDesc,
              allowShortSell = allowShortSell,
              allowNVDR = allowNVDR,
              allowShortSellOnNVDR = allowShortSellOnNVDR,
              allowTTF = allowTTF,
              isValidForTrading = isValidForTrading,
              isOddLot = isOddLot,
              parValue = parValue,
              sectorNumber = sectorNumber,
              underlyingSecCode = underlyingSecCode,
              underlyingSecName = underlyingSecName,
              maturityDate = maturityDate,
              contractMultiplier = contractMultiplier,
              settlMethod = settlMethod
            )
            sym <- store.getInstrument(oid)
          } yield sym).runToFuture.map(_ shouldBe Right(symbol))
        }
      }
      "updateOrderbook" when {
        "N" should {
          "insert new level without the ask/bid time" in {
            (for {
              _ <- store.updateOrderbook(seq, oid, action.copy(levelUpdateAction = 'N'))
              a <- store.getLastOrderbookItem(symbol)
              _ <- store.updateOrderbook(
                seq,
                oid,
                action
                  .copy(
                    levelUpdateAction = 'N',
                    side = Side('B'),
                    price = bidPrice1,
                    qty = bidQty1,
                    marketTs = bidTime1
                  )
              )
              b <- store.getLastOrderbookItem(symbol)
            } yield (a, b)).runToFuture.map(p =>
              p shouldBe (Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel,
                    bids = Vector(),
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    askTime1,
                    bananaTs
                  )
                )
              ),
              Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel,
                    bids = Vector(Some(bidPrice1, bidQty1, Micro(0L))),
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bidTime1,
                    bananaTs
                  )
                )
              ))
            )
          }
        }
        "U" should {
          "update levels" in {
            (for {
              _ <- store.updateOrderbook(
                seq,
                oid,
                action.copy(
                  price = bidPrice2,
                  qty = bidQty2,
                  marketTs = bidTime2,
                  side = Side('B'),
                  levelUpdateAction = 'U'
                )
              )
              last <- store.getLastOrderbookItem(symbol)
            } yield last).runToFuture.map(p =>
              p shouldBe Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel,
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bids = Vector(Some((bidPrice2, bidQty2, Micro(0L)))),
                    marketTs = bidTime2,
                    bananaTs = bananaTs
                  )
                )
              )
            )
          }
        }
        "D" should {
          "delete a level" in {
            (for {
              _ <- store.updateOrderbook(
                seq,
                oid,
                action.copy(level = 1, numDeletes = 1, levelUpdateAction = 'D', side = Side('B'), marketTs = bidTime3)
              )
              last <- store.getLastOrderbookItem(symbol)
            } yield last).runToFuture.map(p =>
              p shouldBe Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel,
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bids = Vector(),
                    marketTs = bidTime3,
                    bananaTs = bananaTs
                  )
                )
              )
            )
          }
        }
      }
      "updateTicker" when {
        "dealSource is anything (1,2,3,4)" should {
          "insert the data without accumulating the qty" in {
            (for {
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice1,
                q = askQty1,
                aggressor = 'B',
                dealSource = 1,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime,
                askTime1,
                bananaTs
              )
              tq1 <- store.getLastTickerTotalQty(symbol)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice2,
                q = askQty2,
                aggressor = 'B',
                dealSource = 2,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 1,
                askTime2,
                bananaTs
              )
              tq2 <- store.getLastTickerTotalQty(symbol)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice3,
                q = askQty3,
                aggressor = 'B',
                dealSource = 4,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 2,
                askTime3,
                bananaTs
              )
              tq3 <- store.getLastTickerTotalQty(symbol)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice4,
                q = askQty4,
                aggressor = 'B',
                dealSource = 3,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 3,
                askTime3,
                bananaTs
              )
              tq4 <- store.getLastTickerTotalQty(symbol)
            } yield (tq1, tq2, tq3, tq4)).runToFuture
              .map(
                _ shouldBe (Right(Qty(askQty1.value)), Right(Qty(askQty2.value)), Right(Qty(askQty3.value)), Right(
                  Qty(askQty4.value)
                ))
              )
          }
        }
      }
    }

    "fu" when {
      val store = storeFu
      "saveTradableInstrument, getInstrument" should {
        "save the instrument along with other data and get it back" in {
          (for {
            _ <- store.saveTradableInstrument(
              oid = oid,
              symbol = symbol,
              secType = secType,
              secDesc = secDesc,
              allowShortSell = allowShortSell,
              allowNVDR = allowNVDR,
              allowShortSellOnNVDR = allowShortSellOnNVDR,
              allowTTF = allowTTF,
              isValidForTrading = isValidForTrading,
              isOddLot = isOddLot,
              parValue = parValue,
              sectorNumber = sectorNumber,
              underlyingSecCode = underlyingSecCode,
              underlyingSecName = underlyingSecName,
              maturityDate = maturityDate,
              contractMultiplier = contractMultiplier,
              settlMethod = settlMethod
            )
            sym <- store.getInstrument(oid)
          } yield sym).runToFuture.map(_ shouldBe Right(symbol))
        }
      }
      "updateOrderbook" when {
        "N" should {
          "insert new level without the ask/bid time" in {
            (for {
              _ <- store.updateOrderbook(seq, oid, action.copy(levelUpdateAction = 'N'))
              a <- store.getLastOrderbookItem(symbol)
              _ <- store.updateOrderbook(
                seq,
                oid,
                action
                  .copy(
                    levelUpdateAction = 'N',
                    side = Side('B'),
                    maxLevel = 5,
                    price = bidPrice1,
                    qty = bidQty1,
                    marketTs = bidTime1
                  )
              )
              b <- store.getLastOrderbookItem(symbol)
            } yield (a, b)).runToFuture.map(p =>
              p shouldBe (Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel = 5,
                    bids = Vector(),
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    askTime1,
                    bananaTs
                  )
                )
              ),
              Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel = 5,
                    bids = Vector(Some(bidPrice1, bidQty1, Micro(0L))),
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bidTime1,
                    bananaTs
                  )
                )
              ))
            )
          }
        }
        "U" should {
          "update levels" in {
            (for {
              _ <- store.updateOrderbook(
                seq,
                oid,
                action.copy(
                  price = bidPrice2,
                  qty = bidQty2,
                  maxLevel = 5,
                  marketTs = bidTime2,
                  side = Side('B'),
                  levelUpdateAction = 'U'
                )
              )
              last <- store.getLastOrderbookItem(symbol)
            } yield last).runToFuture.map(p =>
              p shouldBe Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel = 5,
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bids = Vector(Some((bidPrice2, bidQty2, Micro(0L)))),
                    marketTs = bidTime2,
                    bananaTs = bananaTs
                  )
                )
              )
            )
          }
        }
        "D" should {
          "delete a level" in {
            (for {
              _ <- store.updateOrderbook(
                seq,
                oid,
                action.copy(
                  level = 1,
                  numDeletes = 1,
                  levelUpdateAction = 'D',
                  side = Side('B'),
                  maxLevel = 5,
                  marketTs = bidTime3
                )
              )
              last <- store.getLastOrderbookItem(symbol)
            } yield last).runToFuture.map(p =>
              p shouldBe Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel = 5,
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bids = Vector(),
                    marketTs = bidTime3,
                    bananaTs = bananaTs
                  )
                )
              )
            )
          }
        }
      }
      "updateTicker" when {
        "dealSource is anything (1,2,3,4)" should {
          "insert the data without accumulating the qty" in {
            (for {
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice1,
                q = askQty1,
                aggressor = 'B',
                dealSource = 1,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime,
                askTime1,
                bananaTs
              )
              tq1 <- store.getLastTickerTotalQty(symbol)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice2,
                q = askQty2,
                aggressor = 'B',
                dealSource = 2,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 1,
                askTime2,
                bananaTs
              )
              tq2 <- store.getLastTickerTotalQty(symbol)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice3,
                q = askQty3,
                aggressor = 'B',
                dealSource = 4,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 2,
                askTime3,
                bananaTs
              )
              tq3 <- store.getLastTickerTotalQty(symbol)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice4,
                q = askQty4,
                aggressor = 'B',
                dealSource = 3,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 3,
                askTime3,
                bananaTs
              )
              tq4 <- store.getLastTickerTotalQty(symbol)
            } yield (tq1, tq2, tq3, tq4)).runToFuture
              .map(
                _ shouldBe(Right(Qty(askQty1.value)), Right(Qty(askQty2.value)), Right(Qty(askQty3.value)), Right(
                  Qty(askQty4.value)
                ))
              )
          }
        }
      }
    }
  }
}

object MySQLImplSpec {

  val mysqlConfig: MySqlConfig = MySqlConfig("localhost", 3306, None)
  val storeEq: MySQLImpl       = Store.mysql(channel, mysqlConfig).asInstanceOf[MySQLImpl]
  val storeFu: MySQLImpl       = Store.mysql(Channel.fu, mysqlConfig).asInstanceOf[MySQLImpl]
}
