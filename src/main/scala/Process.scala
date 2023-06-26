package com.guardian

import Config.Channel
import entity._
import repo.Store

import cats.data.EitherT
import com.nasdaq.ouchitch.itch.impl.ItchMessageFactorySet
import genium.trading.itch42.messages._
import monix.eval.Task
import org.apache.logging.log4j.scala.Logging

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters.CollectionHasAsScala

case class Process(store: Store) extends Logging {
  private val messageFactory  = new ItchMessageFactorySet()
  val channel: Config.Channel = store.channel

  def process(seq: Long, bytes: Array[Byte]): Task[Either[AppError, Unit]] =
    for {
      now <- Task.now(Micro(System.currentTimeMillis() * 1000))
      _   <- Task(logger.info(s"seq $seq"))
      msg <- Task(messageFactory.parse(ByteBuffer.wrap(bytes)))
      res <- msg match {
        case a: SecondsMessage => store.saveSecond(a.getSeconds)

        case a: OrderBookDirectoryMessageSetImpl =>
          (for {
            msc <- EitherT(store.getSecond)
            ns  <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            _ <-
              if (channel == Channel.fu && a.getMarketCode == 11) { // block underlyings in futures
                EitherT.rightT[Task, AppError](())
              }
              else {
                for {
                  oid <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
                  _ <- EitherT(
                    store.saveTradableInstrument(
                      oid,
                      Instrument(a.getSymbol),
                      secType = new String(a.getFinancialProduct), // both
                      secDesc = new String(a.getLongName),
                      allowShortSell = a.getAllowShortSell,
                      allowNVDR = a.getAllowNvdr,
                      allowShortSellOnNVDR = a.getAllowShortSellOnNvdr,
                      allowTTF = a.getAllowTtf,
                      isValidForTrading = a.getStatus,
                      lotRoundSize = a.getRoundLotSize,
                      parValue = a.getParValue,
                      sectorNumber = new String(a.getSectorCode),
                      underlyingSecCode = a.getUnderlying, // or underlyingOrderbookId
                      underlyingSecName = new String(a.getUnderlyingName),
                      maturityDate = a.getExpirationDate, // YYYYMMDD
                      contractMultiplier = a.getContractSize,
                      settlMethod = "NA",
                      decimalsInPrice = a.getDecimalsInPrice,
                      marketTs = ns
                    )
                  )
                  _ <- EitherT(store.saveDecimalsInPrice(oid, a.getDecimalsInPrice))
                } yield ()
              }
          } yield ())
            .recoverWith(p => {
              logger.info(p.msg)
              EitherT.rightT[Task, AppError](())
            })
            .value

        case a: MarketByPriceMessage =>
          (for {
            oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
            symbol <- EitherT(store.getInstrument(oid))
            msc    <- EitherT(store.getSecond)
            ns     <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            dec    <- EitherT(store.getDecimalsInPrice(oid))
            acts <- EitherT.rightT[Task, AppError](
              a.getItems.asScala
                .map(p =>
                  FlatPriceLevelAction(
                    oid = OrderbookId(a.getOrderBookId),
                    symbol = symbol,
                    marketTs = ns,
                    bananaTs = now,
                    maxLevel = a.getMaximumLevel,
                    price = Price(p.getPrice),
                    qty = Qty(p.getQuantity),
                    level = p.getLevel,
                    side = Side(p.getSide),
                    levelUpdateAction = p.getLevelUpdateAction.asInstanceOf[Char],
                    numDeletes = p.getNumberOfDeletes
                  )
                )
                .toVector
            )
            _ <- EitherT.right[AppError](store.updateOrderbook(seq, oid, acts, dec))
          } yield ()).value

        case a: TradeTickerMessageSet =>
          (for {
            oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
            symbol <- EitherT(store.getInstrument(oid))
            msc    <- EitherT(store.getSecond)
            ns     <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            dec    <- EitherT(store.getDecimalsInPrice(oid))
            _ <- EitherT(
              store.updateTicker(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = Price(a.getPrice),
                q = Qty(a.getQuantity),
                aggressor = a.getAggressor,
                dealSource = a.getDealSource,
                action = a.getAction,
                tradeReportCode = a.getTradeReportCode,
                dealDateTime = a.getDealDateTime, //nanosec
                decimalsInPrice = dec,
                marketTs = ns,
                bananaTs = now
              )
            )
          } yield ()).value

        case a: EquilibriumPriceMessage =>
          (for {
            oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
            symbol <- EitherT(store.getInstrument(oid))
            msc    <- EitherT(store.getSecond)
            ns     <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            matchedVol <- EitherT.rightT[Task, AppError](
              Qty(if (a.getAskQuantity < a.getBidQuantity) a.getAskQuantity else a.getBidQuantity)
            )
            imbalanceQty <- EitherT.rightT[Task, AppError](Qty(a.getBidQuantity - a.getAskQuantity))
            dec          <- EitherT(store.getDecimalsInPrice(oid))
            _ <- EitherT(
              store.updateProjected(
                oid = oid,
                symbol = symbol,
                seq = seq,
                p = Price(a.getPrice),
                q = matchedVol,
                ib = imbalanceQty,
                decimalsInPrice = dec,
                marketTs = ns,
                bananaTs = now
              )
            )
          } yield ()).value

        case a: TradeStatisticsMessageSet =>
          (for {
            oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
            symbol <- EitherT(store.getInstrument(oid))
            msc    <- EitherT(store.getSecond)
            ns     <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            dec    <- EitherT(store.getDecimalsInPrice(oid))
            _ <- EitherT(
              store.updateTradeStat(
                oid = oid,
                symbol = symbol,
                seq = seq,
                o = Price(a.getOpenPrice),
                h = Price(a.getHighPrice),
                l = Price(a.getLowPrice),
                c = Price(a.getLastPrice),
                lastAuctionPx = Price(a.getLastAuctionPrice),
                avgpx = Price(a.getAveragePrice),
                turnOverQty = Qty(a.getTurnOverQuantity),
                turnOverVal = Price8(a.getTurnOverValue),
                reportedTurnOverQty = Qty(a.getReportedTurnOverQuantity),
                reportedTurnOverVal = Price8(a.getTurnOverValue),
                totalNumberTrades = Qty(a.getTotalNumberOfTrades),
                decimalsInPrice = dec,
                marketTs = ns,
                bananaTs = now
              )
            )
          } yield ()).value

        case a: IndexPriceMessageSet =>
          (for {
            oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
            symbol <- EitherT(store.getInstrument(oid))
            msc    <- EitherT(store.getSecond)
            ns     <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            dec    <- EitherT(store.getDecimalsInPrice(oid))
            _ <- EitherT(
              store.updateMarketStats(
                oid = oid,
                symbol = symbol,
                seq = seq,
                o = Price8(a.getOpenValue),
                h = Price8(a.getHighValue),
                l = Price8(a.getLowValue),
                c = Price8(a.getValue),
                previousClose = Price8(a.getPreviousClose),
                tradedValue = Price8(a.getTradedValue),
                tradedVol = Qty(a.getTradedVolume),
                change = Price8(a.getChange),
                changePercent = a.getChangePercent,
                tradeTs = a.getTimestamp,
                decimalsInPrice = dec,
                marketTs = ns,
                bananaTs = now
              )
            )
          } yield ()).value

        case a: ReferencePriceMessage =>
          (for {
            msc    <- EitherT(store.getSecond)
            ns     <- EitherT.rightT[Task, AppError](Nano.fromSecondAndNano(msc, a.getNanos))
            oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
            symbol <- EitherT(store.getInstrument(oid))
            dec    <- EitherT(store.getDecimalsInPrice(oid))
            _ <- EitherT(
              store.updateReferencePrice(
                oid = oid,
                symbol = symbol,
                priceType = a.getPriceType,
                price = Price(a.getPrice),
                decimalsInPrice = dec,
                marketTs = ns,
                bananaTs = now
              )
            )
          } yield ()).value

        case _ =>
          (for {
            _ <- EitherT.rightT[Task, AppError](logger.info(s"Unparsed message: ${msg.getClass}"))
          } yield ()).value
      }
    } yield res
}