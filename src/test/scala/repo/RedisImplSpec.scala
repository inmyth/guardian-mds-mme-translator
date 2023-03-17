package com.guardian
package repo

import Config.RedisConfig
import Fixtures._
import entity.{Micro, Qty}

import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{Limit, Range}
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

class RedisImplSpec extends AsyncWordSpec with Matchers {
  import RedisImplSpec._
  implicit val ec: Scheduler = monix.execution.Scheduler.Implicits.global

  "RedisImpl" when {
    "connect and flushAll" should {
      "connect to db and clear all data" in {
        (for {
          _ <- store.connect(false)
          _ <- store.asInstanceOf[RedisImpl].flushAll
        } yield ()).runToFuture.map(_ shouldBe ())
      }
    }
    "saveTradableInstrument, getInstrument" should {
      "save the instrument and retrieve it" in {
        (for {
          _ <- store.asInstanceOf[RedisImpl].flushAll
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
            lotRoundSize = lotRoundSize,
            parValue = parValue,
            sectorNumber = sectorNumber,
            underlyingSecCode = underlyingSecCode,
            underlyingSecName = underlyingSecName,
            maturityDate = maturityDate,
            contractMultiplier = contractMultiplier,
            settlMethod = settlMethod,
            marketTs: Micro
          )
          symbol <- store.getInstrument(oid)
        } yield symbol).runToFuture.map(_ shouldBe Right(symbol))
      }
    }
    "saveSecond, getSecond" should {
      "save the unix second and retrieve it" in {
        (for {
          _   <- store.saveSecond(second)
          res <- store.getSecond
        } yield res).runToFuture.map(_ shouldBe Right(second))
      }
    }
    "updateOrderbook" when {
      "N" in {
        (for {
          _    <- store.updateOrderbook(seq, oid, action.copy(levelUpdateAction = 'N'))
          last <- store.getLastOrderbookItem(symbol)
        } yield last).runToFuture.map(p =>
          p shouldBe Right(
            Some(
              OrderbookItem(
                seq,
                maxLevel,
                bids = Vector(),
                asks = Vector(Some((askPrice1, askQty1, askTime1))),
                askTime1,
                bananaTs
              )
            )
          )
        )
      }
      "U" in {
        (for {
          _ <- store.updateOrderbook(
            seq,
            oid,
            action.copy(price = askPrice2, qty = askQty2, marketTs = askTime2, levelUpdateAction = 'U')
          )
          last <- store.getLastOrderbookItem(symbol)
        } yield last).runToFuture.map(p =>
          p shouldBe Right(
            Some(
              OrderbookItem(
                seq,
                maxLevel,
                asks = Vector(Some((askPrice2, askQty2, askTime2))),
                bids = Vector(),
                marketTs = askTime2,
                bananaTs = bananaTs
              )
            )
          )
        )
      }
      "D" in {
        (for {
          _ <- store.updateOrderbook(
            seq,
            oid,
            action.copy(level = 1, numDeletes = 1, levelUpdateAction = 'D', marketTs = askTime3)
          )
          last <- store.getLastOrderbookItem(symbol)
        } yield last).runToFuture.map(p =>
          p shouldBe Right(
            Some(
              OrderbookItem(
                seq,
                maxLevel,
                asks = Vector(),
                bids = Vector(),
                marketTs = askTime3,
                bananaTs = bananaTs
              )
            )
          )
        )
      }
    }
    "updateTicker" when {
      "dealSource is not Trade Report (1,2 4)" should {
        "update tq with the last tq" in {
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
              dealDateTime = dealDateTime,
              askTime2,
              bananaTs
            )
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
              dealDateTime = dealDateTime,
              askTime3,
              bananaTs
            )
            tq <- store.getLastTickerTotalQty(symbol)
          } yield tq).runToFuture.map(_ shouldBe Right(Qty(askQty1.value + askQty2.value + askQty3.value)))
        }
      }
      "dealSource is Trade Report (3)" should {
        "not add anything to the last tq" in {
          (for {
            tq1 <- store.getLastTickerTotalQty(symbol)
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
              dealDateTime = dealDateTime,
              askTime3,
              bananaTs
            )
            tq2 <- store.getLastTickerTotalQty(symbol)
          } yield (tq1, tq2)).runToFuture.map(p => p._1 shouldBe p._2)
        }
      }
    }
    "updateProjected, updateKline, updateMarketStats" in {
      (for {
        _ <- store.updateProjected(
          oid = oid,
          symbol = symbol,
          seq = seq,
          p = askPrice1,
          q = askQty1,
          ib = askQty2,
          marketTs = marketTs,
          bananaTs = bananaTs
        )
        _ <- store.updateKline(
          oid = oid,
          symbol = symbol,
          seq = seq,
          o = askPrice1,
          h = askPrice2,
          l = askPrice3,
          c = askPrice4,
          lastAuctionPx = askPrice5,
          avgpx = askPrice6,
          turnOverQty = askQty1,
          marketTs = marketTs,
          bananaTs = bananaTs
        )
        _ <- store.updateMarketStats(
          oid = oid,
          symbol = symbol,
          seq = seq,
          o = openPriceA,
          h = highPriceA,
          l = lowPriceA,
          c = closePriceA,
          previousClose = prevCloseA,
          tradedVol = askQty6,
          tradedValue = tradedValA,
          change = changeA,
          changePercent = changePercent,
          tradeTs = tradeTs,
          marketTs = marketTs,
          bananaTs = bananaTs
        )
        com <- Task {
          val command = store.asInstanceOf[RedisImpl].getClass.getDeclaredField("commands")
          command.setAccessible(true)
          command.get(store).asInstanceOf[Option[RedisCommands[String, String]]]
        }
        id <- Task {
          val id = store.asInstanceOf[RedisImpl].getClass.getDeclaredField("id")
          id.setAccessible(true)
          id.get(store).asInstanceOf[String]
        }
        prjKey <- Task(store.keyProjected(symbol))
        kliKey <- Task(store.keyKlein(symbol))
        marKey <- Task(store.keyMarketStats(symbol))
        prjId <- Task(
          com.get
            .xrevrange(prjKey, Range.create("-", "+"), Limit.create(0, 1))
            .asScala
            .head
            .getBody
            .asScala(id)
            .toLong
        )
        kliSz <- Task(com.get.xrevrange(kliKey, Range.create("-", "+"), Limit.create(0, 1)).asScala.size)
        marSz <- Task(com.get.xrevrange(marKey, Range.create("-", "+"), Limit.create(0, 1)).asScala.size)
      } yield (prjId, kliSz, marSz)).runToFuture.map(_ shouldBe (seq, 1, 1))
    }
  }
}

object RedisImplSpec {

  val redisConfig: RedisConfig = RedisConfig("localhost", 6379, None)
  val store: Store             = Store.redis(channel, redisConfig)
}
