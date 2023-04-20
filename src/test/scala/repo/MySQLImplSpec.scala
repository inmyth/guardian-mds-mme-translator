package com.guardian
package repo

import Config.{Channel, MySqlConfig}
import Fixtures._
import entity._
import repo.MySQLImpl._

import com.github.jasync.sql.db.general.ArrayRowData
import com.guardian.AppError.SymbolNotFound
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.lang

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.javaapi.FutureConverters

class MySQLImplSpec extends AsyncWordSpec with Matchers {
  import MySQLImplSpec._
  implicit val ec: Scheduler = monix.execution.Scheduler.Implicits.global

  "MqSQLImpl" when {
    "either" should {
      "drop and recreate the db" in {
        (for {
          _ <- dropAllTables(storeEq)
          _ <- storeEq.createTables()
        } yield ()).runToFuture.map(_ shouldBe ())
      }
    }
    "eq" when {
      val store = storeEq
      "saveTradableInstrument, getInstrument, getDecimalsInPrice" should {
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
              lotRoundSize = lotRoundSize,
              parValue = parValue,
              sectorNumber = sectorNumber,
              underlyingSecCode = underlyingSecCode,
              underlyingSecName = underlyingSecName,
              maturityDate = maturityDate,
              contractMultiplier = contractMultiplier,
              settlMethod = settlMethod,
              decimalsInPrice = decimalsInPrice,
              marketTs = marketTs
            )
            sym <- store.getInstrument(oid)
          } yield sym).runToFuture.map(_ shouldBe Right(symbol))
        }
        "return decimalsInPrice" in {
          (for {
            p <- store.getDecimalsInPrice(oid)
          } yield p).runToFuture.map(_ shouldBe Right(decimalsInPrice))
        }
      }
      "updateOrderbook" when {
        "single one action" when {
          "N" should {
            "insert new level without the ask/bid time" in {
              (for {
                _ <- store.updateOrderbook(seq, oid, List(action.copy(levelUpdateAction = 'N')), decimalsInPrice)
                a <- store.getLastOrderbookItem(symbol, decimalsInPrice)
                _ <- store.updateOrderbook(
                  seq,
                  oid,
                  List(
                    action
                      .copy(
                        levelUpdateAction = 'N',
                        side = Side('B'),
                        price = bidPrice1,
                        qty = bidQty1,
                        marketTs = bidTime1
                      )
                  ),
                  decimalsInPrice
                )
                b <- store.getLastOrderbookItem(symbol, decimalsInPrice)
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
                  List(
                    action.copy(
                      price = bidPrice2,
                      qty = bidQty2,
                      marketTs = bidTime2,
                      side = Side('B'),
                      levelUpdateAction = 'U'
                    )
                  ),
                  decimalsInPrice
                )
                last <- store.getLastOrderbookItem(symbol, decimalsInPrice)
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
                  List(
                    action
                      .copy(level = 1, numDeletes = 1, levelUpdateAction = 'D', side = Side('B'), marketTs = bidTime3)
                  ),
                  decimalsInPrice
                )
                last <- store.getLastOrderbookItem(symbol, decimalsInPrice)
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
        "multiple actions at once" should {
          "aggregate all actions and save them as one" in {
            (for {
              _ <- store.updateOrderbook(
                seq,
                oid,
                List(
                  action
                    .copy(
                      level = 1,
                      levelUpdateAction = 'N',
                      side = Side('B'),
                      price = bidPrice1,
                      qty = bidQty1,
                      marketTs = bidTime5
                    ),
                  action.copy(
                    level = 2,
                    levelUpdateAction = 'N',
                    side = Side('B'),
                    price = bidPrice2,
                    qty = bidQty2,
                    marketTs = bidTime6
                  ),
                  action
                    .copy(
                      level = 1,
                      levelUpdateAction = 'U',
                      side = Side('B'),
                      price = bidPrice3,
                      qty = bidQty3,
                      marketTs = bidTime7
                    ),
                  action
                    .copy(
                      level = 2,
                      levelUpdateAction = 'D',
                      numDeletes = 1,
                      side = Side('B'),
                      marketTs = bidTime8
                    )
                ),
                decimalsInPrice
              )
              a <- store.getLastOrderbookItem(symbol, decimalsInPrice)
            } yield a).runToFuture.map(p =>
              p shouldBe Right(
                Some(
                  OrderbookItem(
                    seq,
                    maxLevel,
                    bids = Vector(Some(bidPrice3, bidQty3, Micro(0L))),
                    asks = Vector(Some((askPrice1, askQty1, Micro(0L)))),
                    bananaTs = bananaTs,
                    marketTs = bidTime8
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
        "storing bid or ask aggressor" should {
          "choose the right column to store" in {
            (for {
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice5,
                q = askQty5,
                aggressor = 'A',
                dealSource = 1,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 20,
                askTime5,
                bananaTs
              )
              res1 <- getTickerBidAskAggressor(store, dealDateTime + 20, seq, oid)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice5,
                q = askQty5,
                aggressor = 'B',
                dealSource = 1,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 21,
                askTime5,
                bananaTs
              )
              res2 <- getTickerBidAskAggressor(store, dealDateTime + 21, seq, oid)
              _ <- store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice5,
                q = askQty5,
                aggressor = 'N',
                dealSource = 1,
                action = 1,
                tradeReportCode = 20,
                dealDateTime = dealDateTime + 22,
                askTime5,
                bananaTs
              )
              res3 <- getTickerBidAskAggressor(store, dealDateTime + 22, seq, oid)
            } yield (res1, res2, res3)).runToFuture.map(_ shouldBe ((false, true), (true, false), (false, false)))
          }
        }
      }
      "updateProjected" when {
        "last item is empty" should {
          "insert and not update isFinal" in {
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
              res <- store.getLast2ndProjectedIsFinal
            } yield res).runToFuture.map(_ shouldBe Right(None))
          }
        }
        "last price is not empty and current price is not int.min" should {
          "insert current item only" in {
            (for {
              _ <- store.updateProjected(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = askPrice2,
                q = askQty2,
                ib = askQty2,
                marketTs = new Micro(marketTs.value + 1),
                bananaTs = bananaTs
              )
              res <- store.getLast2ndProjectedIsFinal
            } yield res).runToFuture.map(_ shouldBe Right(Some(false)))
          }
        }
        "last price is not empty and current price is int.min" should {
          "insert current and update the last item as final" in {
            (for {
              _ <- store.updateProjected(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = Price(Int.MinValue),
                q = askQty2,
                ib = askQty2,
                marketTs = Micro(marketTs.value + 2),
                bananaTs = bananaTs
              )
              res <- store.getLast2ndProjectedIsFinal
            } yield res).runToFuture.map(_ shouldBe Right(Some(true)))
          }
        }
        "last price is int.min and current price is int.min" should {
          "insert current only" in {
            (for {
              _ <- store.updateProjected(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = Price(Int.MinValue),
                q = askQty3,
                ib = askQty3,
                marketTs = Micro(marketTs.value + 3),
                bananaTs = bananaTs
              )
              res <- store.getLast2ndProjectedIsFinal
            } yield res).runToFuture.map(_ shouldBe Right(Some(false)))
          }
        }
      }
      "updateMySqlIPOPrice" should {
        "update the ipo price of an instrument in tradable instrument table" in {
          (for {
            _   <- store.updateMySqlIPOPrice(oid, Price(10000))
            res <- getIPOPrice(store, oid)
          } yield res).runToFuture.map(_ shouldBe Price(10000))
        }
      }
      "updateKline (updateEquityDay)" should {
        "create a new item with open price 1" when {
          "last item is empty" in {
            (for {
              _ <- store.updateEquityDay(
                oid = oid,
                symbol = symbol,
                o = openPrice1,
                c = askPrice2,
                h = askPrice3,
                l = askPrice4,
                turnOverQty = askQty1,
                marketTs = marketTs
              )
              res <- store.getEquityDayOf(localDateToMySqlDate(microToSqlDateTime(marketTs).toLocalDate))
            } yield res).runToFuture.map(
              _ shouldBe Right(
                Some(
                  EquityDayItem(
                    dateTime = microToSqlDateTime(marketTs),
                    openPrice1 = Some(openPrice1),
                    openPrice2 = None,
                    closePrice1 = None,
                    closePrice2 = None,
                    vol = Some(askQty1),
                    h = Some(askPrice3),
                    l = Some(askPrice4)
                  )
                )
              )
            )
          }
        }
        "update only trade date and some other data when new item is before 12:30" in {
          val date = microToSqlDateTime(t1225)
          (for {
            _ <- store.updateEquityDay(
              oid = oid,
              symbol = symbol,
              o = askPrice5,
              c = askPrice6,
              h = askPrice7,
              l = askPrice8,
              turnOverQty = askQty2,
              marketTs = t1225
            )
            res <- store.getEquityDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                EquityDayItem(
                  dateTime = microToSqlDateTime(t1225),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = None,
                  closePrice1 = None,
                  closePrice2 = None,
                  vol = Some(askQty2),
                  h = Some(askPrice7),
                  l = Some(askPrice8)
                )
              )
            )
          )
        }
        "update close price 1 and open price 2 when new item is after 12:30 and current item is before 12:30" in {
          val date = microToSqlDateTime(t1235)
          (for {
            _ <- store.updateEquityDay(
              oid = oid,
              symbol = symbol,
              o = openPrice2,
              c = closePrice1,
              h = bidPrice3,
              l = bidPrice4,
              turnOverQty = bidQty1,
              marketTs = t1235
            )
            res <- store.getEquityDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                EquityDayItem(
                  dateTime = microToSqlDateTime(t1235),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = None,
                  vol = Some(bidQty1),
                  h = Some(bidPrice3),
                  l = Some(bidPrice4)
                )
              )
            )
          )
        }
        "update only trade date and some other data when new item is between 12:30 and 16:30" in {
          val date = microToSqlDateTime(t1625)
          (for {
            _ <- store.updateEquityDay(
              oid = oid,
              symbol = symbol,
              o = bidPrice5,
              c = bidPrice6,
              h = bidPrice7,
              l = bidPrice8,
              turnOverQty = bidQty2,
              marketTs = t1625
            )
            res <- store.getEquityDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                EquityDayItem(
                  dateTime = microToSqlDateTime(t1625),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = None,
                  vol = Some(bidQty2),
                  h = Some(bidPrice7),
                  l = Some(bidPrice8)
                )
              )
            )
          )
        }
        "update close price 2 if new item is after 16:30 and current item is before 16:30" in {
          val date = microToSqlDateTime(t1635)
          (for {
            _ <- store.updateEquityDay(
              oid = oid,
              symbol = symbol,
              o = openPrice2,
              c = closePrice2,
              h = bidPrice9,
              l = bidPrice10,
              turnOverQty = bidQty2,
              marketTs = t1635
            )
            res <- store.getEquityDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                EquityDayItem(
                  dateTime = microToSqlDateTime(t1635),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = Some(closePrice2),
                  vol = Some(bidQty2),
                  h = Some(bidPrice9),
                  l = Some(bidPrice10)
                )
              )
            )
          )
        }
      }
      "updateMySqlSettlementPrice" should {
        "update settlement price in EquityDay table" in {
          (for {
            _   <- store.updateMySqlSettlementPrice(oid, t1635, settlPrice)
            res <- store.getEquityDayOf(localDateToMySqlDate(microToSqlDateTime(t1635).toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                EquityDayItem(
                  dateTime = microToSqlDateTime(t1635),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = Some(closePrice2),
                  vol = Some(bidQty2),
                  h = Some(bidPrice9),
                  l = Some(bidPrice10),
                  settlPrice = Some(settlPrice)
                )
              )
            )
          )
        }
      }
      "updateMarketStats (updateIndexDay and updateIndexTicker)" should {
        "update index day once a day, update index ticker" in {
          (for {
            _ <- store.updateMarketStats(
              oid = oid,
              symbol = symbol,
              seq = seq,
              o = openPriceA,
              c = closePriceA,
              h = highPriceA,
              l = lowPriceA,
              previousClose = prevCloseA,
              tradedVol = tradedVolA,
              tradedValue = tradedValA,
              change = changeA,
              changePercent = 20,
              tradeTs = tradeTs,
              marketTs = marketTs,
              bananaTs = bananaTs
            )
            res1 <- getIndexDayOpenPrice(store, marketTs, oid)
            res2 <- getIndexTickerOpenPrice(store, marketTs, oid)
          } yield (res1, res2)).runToFuture.map(_ shouldBe (Some(openPriceA), Some(openPriceA)))
        }
        "not update index day if data of that day exists, update index ticker" in {
          val marketTs2 = Micro(marketTs.value + 1000000000)
          (for {
            _ <- store.updateMarketStats(
              oid = oid,
              symbol = symbol,
              seq = seq,
              o = openPriceB,
              c = closePriceA,
              h = highPriceA,
              l = lowPriceA,
              previousClose = prevCloseA,
              tradedVol = tradedVolA,
              tradedValue = tradedValA,
              change = changeA,
              changePercent = 20,
              tradeTs = tradeTs,
              marketTs = marketTs2,
              bananaTs = bananaTs
            )
            res1 <- getIndexDayOpenPrice(store, marketTs2, oid)
            res2 <- getIndexTickerOpenPrice(store, marketTs2, oid)
          } yield (res1, res2)).runToFuture.map(_ shouldBe (Some(openPriceA), Some(openPriceB)))
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
              lotRoundSize = lotRoundSize,
              parValue = parValue,
              sectorNumber = sectorNumber,
              underlyingSecCode = underlyingSecCode,
              underlyingSecName = underlyingSecName,
              maturityDate = maturityDate,
              contractMultiplier = contractMultiplier,
              settlMethod = settlMethod,
              decimalsInPrice = decimalsInPrice,
              marketTs = marketTs
            )
            sym <- store.getInstrument(oid)
          } yield sym).runToFuture.map(_ shouldBe Right(symbol))
        }
      }
      "updateOrderbook" when {
        "N" should {
          "insert new level without the ask/bid time" in {
            (for {
              _ <- store.updateOrderbook(seq, oid, List(action.copy(levelUpdateAction = 'N')), decimalsInPrice)
              a <- store.getLastOrderbookItem(symbol, decimalsInPrice)
              _ <- store.updateOrderbook(
                seq,
                oid,
                List(
                  action
                    .copy(
                      levelUpdateAction = 'N',
                      side = Side('B'),
                      maxLevel = 5,
                      price = bidPrice1,
                      qty = bidQty1,
                      marketTs = bidTime1
                    )
                ),
                decimalsInPrice
              )
              b <- store.getLastOrderbookItem(symbol, decimalsInPrice)
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
                List(
                  action.copy(
                    price = bidPrice2,
                    qty = bidQty2,
                    maxLevel = 5,
                    marketTs = bidTime2,
                    side = Side('B'),
                    levelUpdateAction = 'U'
                  )
                ),
                decimalsInPrice
              )
              last <- store.getLastOrderbookItem(symbol, decimalsInPrice)
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
                List(
                  action.copy(
                    level = 1,
                    numDeletes = 1,
                    levelUpdateAction = 'D',
                    side = Side('B'),
                    maxLevel = 5,
                    marketTs = bidTime3
                  )
                ),
                decimalsInPrice
              )
              last <- store.getLastOrderbookItem(symbol, decimalsInPrice)
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
                _ shouldBe (Right(Qty(askQty1.value)), Right(Qty(askQty2.value)), Right(Qty(askQty3.value)), Right(
                  Qty(askQty4.value)
                ))
              )
          }
        }
      }
      "updateKline (updateEquityDay)" should {
        "create a new item with open price 1" when {
          "last item is empty" in {
            (for {
              _ <- store.updateDerivativeDay(
                oid = oid,
                symbol = symbol,
                o = openPrice1,
                c = askPrice2,
                h = askPrice3,
                l = askPrice4,
                turnOverQty = askQty1,
                marketTs = marketTs
              )
              res <- store.getDerivativeDayOf(localDateToMySqlDate(microToSqlDateTime(marketTs).toLocalDate))
            } yield res).runToFuture.map(
              _ shouldBe Right(
                Some(
                  DerivativeDayItem(
                    dateTime = microToSqlDateTime(marketTs),
                    openPrice1 = Some(openPrice1),
                    openPrice2 = None,
                    closePrice1 = None,
                    closePrice2 = None,
                    openNightPrice = None,
                    closeNightPrice = None,
                    vol = Some(askQty1),
                    h = Some(askPrice3),
                    l = Some(askPrice4)
                  )
                )
              )
            )
          }
        }
        "update only trade date and some other data when new item is before 12:30" in {
          val date = microToSqlDateTime(t1225)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = askPrice5,
              c = askPrice6,
              h = askPrice7,
              l = askPrice8,
              turnOverQty = askQty2,
              marketTs = t1225
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t1225),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = None,
                  closePrice1 = None,
                  closePrice2 = None,
                  openNightPrice = None,
                  closeNightPrice = None,
                  vol = Some(askQty2),
                  h = Some(askPrice7),
                  l = Some(askPrice8)
                )
              )
            )
          )
        }
        "update close price 1 and open price 2 when new item is after 12:30 and current item is before it" in {
          val date = microToSqlDateTime(t1235)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = openPrice2,
              c = closePrice1,
              h = bidPrice3,
              l = bidPrice4,
              turnOverQty = bidQty1,
              marketTs = t1235
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t1235),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = None,
                  openNightPrice = None,
                  closeNightPrice = None,
                  vol = Some(bidQty1),
                  h = Some(bidPrice3),
                  l = Some(bidPrice4)
                )
              )
            )
          )
        }
        "update only trade date and some other data when new item is between 12:30 and 16:30" in {
          val date = microToSqlDateTime(t1625)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = bidPrice5,
              c = bidPrice6,
              h = bidPrice7,
              l = bidPrice8,
              turnOverQty = bidQty2,
              marketTs = t1625
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t1625),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = None,
                  openNightPrice = None,
                  closeNightPrice = None,
                  vol = Some(bidQty2),
                  h = Some(bidPrice7),
                  l = Some(bidPrice8)
                )
              )
            )
          )
        }
        "update close price 2 if new item is after 16:30 and current item is before it" in {
          val date = microToSqlDateTime(t1635)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = openPrice2,
              c = closePrice2,
              h = bidPrice9,
              l = bidPrice10,
              turnOverQty = bidQty2,
              marketTs = t1635
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t1635),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = Some(closePrice2),
                  openNightPrice = None,
                  closeNightPrice = None,
                  vol = Some(bidQty2),
                  h = Some(bidPrice9),
                  l = Some(bidPrice10)
                )
              )
            )
          )
        }
        "update night open price if new item is after 18:45 and current item is before it" in {
          val date = microToSqlDateTime(t1850)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = nightOpenPrice,
              c = closePrice2,
              h = bidPrice9,
              l = bidPrice10,
              turnOverQty = bidQty2,
              marketTs = t1850
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t1850),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = Some(closePrice2),
                  openNightPrice = Some(nightOpenPrice),
                  closeNightPrice = None,
                  vol = Some(bidQty2),
                  h = Some(bidPrice9),
                  l = Some(bidPrice10)
                )
              )
            )
          )
        }
        "update only trade date and some other data when new item is between 18:40 and 23:30" in {
          val date = microToSqlDateTime(t2325)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = bidPrice5,
              c = bidPrice6,
              h = bidPrice7,
              l = bidPrice8,
              turnOverQty = bidQty2,
              marketTs = t2325
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t2325),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = Some(closePrice2),
                  openNightPrice = Some(nightOpenPrice),
                  closeNightPrice = None,
                  vol = Some(bidQty2),
                  h = Some(bidPrice7),
                  l = Some(bidPrice8)
                )
              )
            )
          )
        }
        "update night close price if new item is after 23:30 and current item is before it" in {
          val date = microToSqlDateTime(t2335)
          (for {
            _ <- store.updateDerivativeDay(
              oid = oid,
              symbol = symbol,
              o = nightOpenPrice,
              c = nightClosePrice,
              h = bidPrice9,
              l = bidPrice10,
              turnOverQty = bidQty2,
              marketTs = t2335
            )
            res <- store.getDerivativeDayOf(localDateToMySqlDate(date.toLocalDate))
          } yield res).runToFuture.map(
            _ shouldBe Right(
              Some(
                DerivativeDayItem(
                  dateTime = microToSqlDateTime(t2335),
                  openPrice1 = Some(openPrice1),
                  openPrice2 = Some(openPrice2),
                  closePrice1 = Some(closePrice1),
                  closePrice2 = Some(closePrice2),
                  openNightPrice = Some(nightOpenPrice),
                  closeNightPrice = Some(nightClosePrice),
                  vol = Some(bidQty2),
                  h = Some(bidPrice9),
                  l = Some(bidPrice10)
                )
              )
            )
          )
        }
      }
      "getInstrument" when {
        "symbol exists" should {
          "return right" in {
            (for {
              res <- store.getInstrument(orderbookId = oid)
            } yield res).runToFuture.map(
              _ shouldBe Right(symbol)
            )
          }
        }
        "no symbol exists" should {
          "return Left" in {
            (for {
              res <- store.getInstrument(orderbookId = OrderbookId(-1))
            } yield res).runToFuture.map(
              _ shouldBe Left(SymbolNotFound(OrderbookId(-1)))
            )
          }
        }
      }
    }
  }
}

object MySQLImplSpec {

  val mysqlConfig: MySqlConfig = MySqlConfig("localhost", 3306, None, None)
  val storeEq: MySQLImpl       = Store.mysql(channel, mysqlConfig).asInstanceOf[MySQLImpl]
  val storeFu: MySQLImpl       = Store.mysql(Channel.fu, mysqlConfig).asInstanceOf[MySQLImpl]

  def dropAllTables(store: MySQLImpl): Task[Unit] =
    for {
      drop <- Task.now(s"""
           |DROP DATABASE mdsdb;
           |""".stripMargin)
      _    <- Task.fromFuture(FutureConverters.asScala(store.connection.sendPreparedStatement(drop)))
    } yield ()

  def getIndexTickerOpenPrice(store: MySQLImpl, ts: Micro, oid: OrderbookId): Task[Option[Price8]] =
    for {
      marketDt <- Task(microToSqlDateTime(ts))
      sql      <- Task(s"""
           |SELECT OpenPrice FROM ${storeEq.indexTickerTable} WHERE TradeDate = ? AND SecCode = ?
           |""".stripMargin)
      data <- Task.fromFuture(
        FutureConverters.asScala(store.connection.sendPreparedStatement(sql, Vector(marketDt, oid.value).asJava))
      )
      res <- Task(
        data.getRows
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => Price8(p.getLong("OpenPrice")))
          .headOption
      )
    } yield res

  def getIndexDayOpenPrice(store: MySQLImpl, ts: Micro, oid: OrderbookId): Task[Option[Price8]] =
    for {
      marketDt <- Task(microToSqlDateTime(ts))
      day      <- Task(localDateToMySqlDate(marketDt.toLocalDate))
      sql      <- Task(s"""
           |SELECT OpenPrice FROM ${storeEq.indexDayTable} WHERE TradeDate
           |LIKE "${day.toString}%" AND SecCode = ${oid.value}
           |""".stripMargin)
      data     <- Task.fromFuture(FutureConverters.asScala(store.connection.sendPreparedStatement(sql, Vector().asJava)))
      res <- Task(
        data.getRows
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => Price8(p.getLong("OpenPrice")))
          .headOption
      )
    } yield res

  def getIPOPrice(store: MySQLImpl, oid: OrderbookId): Task[Price] =
    for {
      data <- Task.fromFuture(
        FutureConverters.asScala(
          store.connection.sendPreparedStatement(
            s"SELECT IPOPrice, SecCode from ${store.tradableInstrumentTable} WHERE SecCode = ${oid.value}",
            Vector().asJava
          )
        )
      )
      res <- Task(
        data.getRows
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => Price(p.getInt("IPOPrice")))
          .head
      )
    } yield res
  def getTickerBidAskAggressor(
      store: MySQLImpl,
      tradeTime: Long,
      seq: Long,
      oid: OrderbookId
  ): Task[(lang.Boolean, lang.Boolean)] =
    for {
      data <- Task.fromFuture(
        FutureConverters.asScala(
          store.connection.sendPreparedStatement(
            s"""SELECT BidAggressor, AskAggressor, TradeTime, SeqNo, SecCode from ${store.tickerTable} WHERE
             |TradeTime = $tradeTime AND SecCode = ${oid.value} AND SeqNo = $seq""".stripMargin,
            Vector().asJava
          )
        )
      )
      res <- Task(
        data.getRows
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => (p.getBoolean("BidAggressor"), p.getBoolean("AskAggressor")))
          .head
      )
    } yield res
}
