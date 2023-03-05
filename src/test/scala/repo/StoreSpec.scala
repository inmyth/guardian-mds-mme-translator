package com.guardian
package repo

import Fixtures._
import entity.Qty
import repo.InMemImpl.{KlineItem, MarketStatsItem, ProjectedItem}

import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class StoreSpec extends AsyncWordSpec with Matchers {
  implicit val ec: Scheduler = monix.execution.Scheduler.Implicits.global
  import StoreSpec._

  "store" when {
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
    "updateProjected" in {
      (for {
        _ <-
          store
            .updateProjected(
              oid = oid,
              symbol = symbol,
              seq = seq,
              p = askPrice1,
              q = askQty1,
              ib = askQty2,
              marketTs = marketTs,
              bananaTs = bananaTs
            )
        db <- Task {
          val x = store.asInstanceOf[InMemImpl].getClass.getDeclaredField("projectedDb")
          x.setAccessible(true)
          x.get(store).asInstanceOf[Map[String, Vector[ProjectedItem]]]
        }
      } yield db).runToFuture.map(
        _.head._2 shouldBe Vector(
          ProjectedItem(
            seq = seq,
            p = askPrice1,
            q = askQty1,
            ib = askQty2,
            marketTs = marketTs,
            bananaTs = bananaTs
          )
        )
      )
    }
    "updateKline" in {
      (for {
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
        db <- Task {
          val x = store.asInstanceOf[InMemImpl].getClass.getDeclaredField("klineDb")
          x.setAccessible(true)
          x.get(store).asInstanceOf[Map[String, Vector[KlineItem]]]
        }
      } yield db).runToFuture.map(
        _.head._2 shouldBe Vector(
          KlineItem(
            seq = seq,
            o = askPrice1,
            h = askPrice2,
            l = askPrice3,
            c = askPrice4,
            lauctpx = askPrice5,
            avgpx = askPrice6,
            turnOverQty = askQty1,
            marketTs = marketTs,
            bananaTs = bananaTs
          )
        )
      )
    }
    "updateMarketStats" in {
      (for {
        _ <- store.updateMarketStats(
          oid = oid,
          symbol = symbol,
          seq = seq,
          o = askQty1,
          h = askQty2,
          l = askQty3,
          c = askQty4,
          previousClose = askQty5,
          tradedVol = askQty6,
          tradedValue = askQty7,
          change = change,
          changePercent = changePercent,
          tradeTs = tradeTs,
          marketTs = marketTs,
          bananaTs = bananaTs
        )
        db <- Task {
          val x = store.asInstanceOf[InMemImpl].getClass.getDeclaredField("marketStatsDb")
          x.setAccessible(true)
          x.get(store).asInstanceOf[Map[String, Vector[MarketStatsItem]]]
        }
      } yield db).runToFuture.map(
        _.head._2 shouldBe Vector(
          MarketStatsItem(
            seq = seq,
            o = askQty1,
            h = askQty2,
            l = askQty3,
            c = askQty4,
            previousClose = askQty5,
            tradedVol = askQty6,
            tradedValue = askQty7,
            change = change,
            changePercent = changePercent,
            tradeTs = tradeTs,
            marketTs = marketTs,
            bananaTs = bananaTs
          )
        )
      )
    }
  }
}

object StoreSpec {

  val store: Store = new InMemImpl(channel)
}
