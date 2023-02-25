package com.guardian
package repo

import com.guardian.Config.{Channel, MySqlConfig}
import monix.execution.Scheduler
import Fixtures._

import com.guardian.entity.{Micro, Side}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

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
      "saveTradableInstrument, getInstrument" should {
        "save the instrument along with other data and get it back" in {
          (for {
            _ <- storeEq.saveTradableInstrument(
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
            sym <- storeEq.getInstrument(oid)
          } yield sym).runToFuture.map(_ shouldBe Right(symbol))
        }
      }
    }
    "updateOrderbook" when {
      "N" should {
        "insert new level without the ask/bid time" in {
          (for {
            _ <- storeEq.updateOrderbook(seq, oid, action.copy(levelUpdateAction = 'N'))
            a <- storeEq.getLastOrderbookItem(symbol)
            _ <- storeEq.updateOrderbook(
              seq,
              oid,
              action
                .copy(levelUpdateAction = 'N', side = Side('B'), price = bidPrice1, qty = bidQty1, marketTs = bidTime1)
            )
            b <- storeEq.getLastOrderbookItem(symbol)
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
            _ <- storeEq.updateOrderbook(
              seq,
              oid,
              action.copy(price = bidPrice2, qty = bidQty2, marketTs = bidTime2, side = Side('B'), levelUpdateAction = 'U')
            )
            last <- storeEq.getLastOrderbookItem(symbol)
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
            _ <- storeEq.updateOrderbook(
              seq,
              oid,
              action.copy(level = 1, numDeletes = 1, levelUpdateAction = 'D', side = Side('B'), marketTs = bidTime3)
            )
            last <- storeEq.getLastOrderbookItem(symbol)
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
    "fu" when {
      "saveTradableInstrument, getInstrument" should {
        "save the instrument along with other data and get it back" in {
          (for {
            _ <- storeFu.saveTradableInstrument(
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
            sym <- storeFu.getInstrument(oid)
          } yield sym).runToFuture.map(_ shouldBe Right(symbol))
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
