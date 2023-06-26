package com.guardian
package repo

import AppError.{RedisConnectionError, SecondNotFound, SymbolNotFound}
import Config.Channel
import entity._

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{Limit, Range, RedisClient}
import monix.eval.Task

import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, MapHasAsScala}
import scala.util.{Failure, Success, Try}

class RedisImpl(channel: Channel, client: RedisClient) extends Store(channel) {
  private var connection: Option[StatefulRedisConnection[String, String]] = Option.empty
  private var commands: Option[RedisCommands[String, String]]             = Option.empty

  val keyTradableInstrument                                  = s"${channel.toString}:symbol_reference"
  val keyOrderbook: Instrument => String                     = (symbol: Instrument) => s"${channel.toString}:orderbook:${symbol.value}"
  val keyTicker: Instrument => String                        = (symbol: Instrument) => s"${channel.toString}:tick:${symbol.value}"
  val keyProjected: Instrument => String                     = (symbol: Instrument) => s"${channel.toString}:projected:${symbol.value}"
  val keyStat: Instrument => String                          = (symbol: Instrument) => s"${channel.toString}:stat:${symbol.value}"
  val keyMarketStats: Instrument => String                   = (symbol: Instrument) => s"id:mkt:${symbol.value}"
  val keyDecimalsInPrice: String                             = s"${channel.toString}:decimalsInPrice"
  val keyReferencePrice: Instrument => String                = (symbol: Instrument) => s"${channel.toString}:refprice:${symbol.value}"
  val cacheInstrument: mutable.Map[OrderbookId, Instrument]  = mutable.HashMap.empty
  val cacheOrderbook: mutable.Map[Instrument, OrderbookItem] = mutable.HashMap.empty
  val cacheDecimalsInPrice: mutable.Map[OrderbookId, Short]  = mutable.HashMap.empty
  var cacheSecond: Option[Int]                               = None

  override def connect(): Task[Either[AppError, Unit]] = {
    Try {
      client.connect()
    } match {
      case Failure(exception) => RedisConnectionError(exception.toString).asLeft.pure[Task]
      case Success(con) =>
        connection = Some(con)
        commands = Some(con.sync())
        ().asRight.pure[Task]
    }
  }

  override def disconnect: Task[Either[AppError, Unit]] = {
    connection.foreach(_.close())
    client.shutdown()
    ().asRight.pure[Task]
  }

  def flushAll: Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](commands.get.flushall())
    } yield ()).value

  val value = "value"
  override def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](this.cacheSecond = Some(unixSecond))
    } yield ()).value

  override def getSecond: Task[Either[AppError, Int]] =
    (for {
      s <- EitherT.fromEither[Task](cacheSecond.fold(SecondNotFound.asLeft[Int])(p => p.asRight))
    } yield s).value

  override def saveTradableInstrument(
      oid: OrderbookId,
      symbol: Instrument,
      secType: String,
      secDesc: String,
      allowShortSell: Byte,
      allowNVDR: Byte,
      allowShortSellOnNVDR: Byte,
      allowTTF: Byte,
      isValidForTrading: Byte,
      lotRoundSize: Int,
      parValue: Long,
      sectorNumber: String,
      underlyingSecCode: Int,
      underlyingSecName: String,
      maturityDate: Int,
      contractMultiplier: Int,
      settlMethod: String,
      decimalsInPrice: Short,
      marketTs: Nano
  ): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](cacheInstrument += oid -> symbol)
      _ <- EitherT.rightT[Task, AppError](commands.get.hset(keyTradableInstrument, oid.value.toString, symbol.value))
    } yield ()).value

  override def getInstrument(oid: OrderbookId): Task[Either[AppError, Instrument]] =
    (for {
      res <-
        EitherT.fromEither[Task](cacheInstrument.get(oid).fold(SymbolNotFound(oid).asLeft[Instrument])(p => p.asRight))
    } yield res).value

  private val id       = "id"
  private val a        = "a"
  private val b        = "b"
  private val aq       = "aq"
  private val bq       = "bq"
  private val at       = "at"
  private val bt       = "bt"
  private val tss      = "tss"
  private val tsb      = "tsb"
  private val maxLevel = "maxLevel"

  override def getLastOrderbookItem(
      symbol: Instrument,
      decimalsInPrice: Short
  ): Task[Either[AppError, Option[OrderbookItem]]] =
    (for {
      res <- EitherT.rightT[Task, AppError](cacheOrderbook.get(symbol))
    } yield res).value

  override def saveOrderbookItem(
      symbol: Instrument,
      orderbookId: OrderbookId,
      item: OrderbookItem,
      decimalsInPrice: Short
  ): Task[Either[AppError, Unit]] =
    (for {
      obk <- EitherT.rightT[Task, AppError](keyOrderbook(symbol))
      _   <- EitherT.rightT[Task, AppError](cacheOrderbook += symbol -> item)
      _ <- EitherT.rightT[Task, AppError] {
        val id       = item.seq
        val maxLevel = item.maxLevel
        val aTup = item.asks.map {
          case Some(value) => (Store.intToBigDecimal(value._1.value, decimalsInPrice), value._2, value._3)
          case None        => (0.0f, Qty(0L), Nano("0"))
        }
        val a  = s"""[${aTup.map(_._1).mkString(",")}]"""
        val aq = s"""[${aTup.map(_._2.value).mkString(",")}]"""
        val at = s"""[${aTup.map(_._3.value).mkString(",")}]"""
        val bTup = item.bids.map {
          case Some(value) => (Store.intToBigDecimal(value._1.value, decimalsInPrice), value._2, value._3)
          case None        => (0.0f, Qty(0L), Nano("0"))
        }
        val b        = s"""[${bTup.map(_._1).mkString(",")}]"""
        val bq       = s"""[${bTup.map(_._2.value).mkString(",")}]"""
        val bt       = s"""[${bTup.map(_._3.value).mkString(",")}]"""
        val marketTs = item.marketTs.value
        val bananaTs = item.bananaTs.value.toString
        commands.get.xadd(
          obk,
          Map(
            this.id       -> id.toString,
            this.maxLevel -> maxLevel.toString,
            this.a        -> a,
            this.aq       -> aq,
            this.at       -> at,
            this.b        -> b,
            this.bq       -> bq,
            this.bt       -> bt,
            this.tss      -> marketTs,
            this.tsb      -> bananaTs
          ).asJava
        )
      }
    } yield ()).value

  val p               = "p"
  val q               = "q"
  val tq              = "tq"
  val s               = "s"
  val action          = "action"
  val tradeReportCode = "trade_report_code"
  val dealSource      = "deal_source"

  override def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]] =
    (for {
      tck <- EitherT.rightT[Task, AppError](keyTicker(symbol))
      res <- EitherT.rightT[Task, AppError](
        commands.get
          .xrevrange(tck, Range.create("-", "+"), Limit.create(0, 1))
          .asScala
          .headOption
          .map(p => {
            val body = p.getBody.asScala
            Qty(body(this.tq).toLong)
          })
          .getOrElse(Qty(0L))
      )
    } yield res).value

  override def saveTicker(
      oid: OrderbookId,
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      aggressor: Byte,
      tq: Qty,
      dealSource: Byte,
      action: Byte,
      tradeReportCode: Short,
      dealDateTime: Long,
      decimalsInPrice: Short,
      marketTs: Nano,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      tck <- EitherT.rightT[Task, AppError](keyTicker(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          tck,
          Map(
            this.id              -> seq.toString,
            this.p               -> Store.intToBigDecimal(p.value, decimalsInPrice).toString,
            this.q               -> q.value.toString,
            this.s               -> aggressor.toChar.toString,
            this.tq              -> tq.value.toString,
            this.action          -> action.toString,
            this.tradeReportCode -> tradeReportCode.toString,
            this.dealSource      -> dealSource.toString,
            this.tss             -> marketTs.value,
            this.tsb             -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  override def updateTicker(
      oid: OrderbookId,
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      aggressor: Byte,
      dealSource: Byte,
      action: Byte,
      tradeReportCode: Short,
      dealDateTime: Long,
      decimalsInPrice: Short,
      marketTs: Nano,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      lastTq <- EitherT(getLastTickerTotalQty(symbol))
      newTq <- EitherT.rightT[Task, AppError](dealSource match {
        case 3 => lastTq // Trade Report
        case _ => Qty(lastTq.value + q.value)
      })
      _ <- EitherT(
        saveTicker(
          oid = oid,
          symbol = symbol,
          seq = seq,
          p = p,
          q = q,
          tq = newTq,
          aggressor = aggressor,
          dealSource = dealSource,
          action = action,
          tradeReportCode = tradeReportCode,
          dealDateTime = dealDateTime,
          decimalsInPrice = decimalsInPrice,
          marketTs = marketTs,
          bananaTs = bananaTs
        )
      )
    } yield ()).value

  val ib = "ib"
  override def updateProjected(
      oid: OrderbookId,
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
      decimalsInPrice: Short,
      marketTs: Nano,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      tck <- EitherT.rightT[Task, AppError](keyProjected(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          tck,
          Map(
            this.id  -> seq.toString,
            this.p   -> Store.intToBigDecimal(p.value, decimalsInPrice).toString,
            this.q   -> q.value.toString,
            this.ib  -> ib.value.toString,
            this.tss -> marketTs.value,
            this.tsb -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  val o                   = "o"
  val h                   = "h"
  val l                   = "l"
  val c                   = "c"
  val lastAuctionPx       = "lauctpx"
  val avgpx               = "avgpx"
  val turnOverQty         = "turnover_qty"
  val turnOverVal         = "turnover_val"
  val reportedTurnOverQty = "reported_turnover_qty"
  val reportedTurnOverVal = "reported_turnover_val"
  val totalNumberTrades   = "total_number_trades"
  override def updateTradeStat(
      oid: OrderbookId,
      symbol: Instrument,
      seq: Long,
      o: Price,
      h: Price,
      l: Price,
      c: Price,
      lastAuctionPx: Price,
      avgpx: Price,
      turnOverQty: Qty,
      turnOverVal: Price8,
      reportedTurnOverQty: Qty,
      reportedTurnOverVal: Price8,
      totalNumberTrades: Qty,
      decimalsInPrice: Short,
      marketTs: Nano,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      klk <- EitherT.rightT[Task, AppError](keyStat(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          klk,
          Map(
            this.o                   -> Store.intToBigDecimal(o.value, decimalsInPrice).toString(),
            this.h                   -> Store.intToBigDecimal(h.value, decimalsInPrice).toString(),
            this.l                   -> Store.intToBigDecimal(l.value, decimalsInPrice).toString(),
            this.c                   -> Store.intToBigDecimal(c.value, decimalsInPrice).toString(),
            this.lastAuctionPx       -> Store.intToBigDecimal(lastAuctionPx.value, decimalsInPrice).toString(),
            this.avgpx               -> Store.intToBigDecimal(avgpx.value, decimalsInPrice).toString(),
            this.turnOverQty         -> turnOverQty.value.toString,
            this.turnOverVal         -> turnOverQty.value.toString,
            this.reportedTurnOverQty -> reportedTurnOverQty.value.toString,
            this.reportedTurnOverVal -> reportedTurnOverVal.value.toString,
            this.totalNumberTrades   -> totalNumberTrades.value.toString,
            this.tss                 -> marketTs.value,
            this.tsb                 -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  private val previousClose = "pc"
  private val tradedVolume  = "vol"
  private val tradeValue    = "val"
  private val change        = "change"
  private val changePercent = "change_percent"
  override def updateMarketStats(
      oid: OrderbookId,
      symbol: Instrument,
      seq: Long,
      o: Price8,
      h: Price8,
      l: Price8,
      c: Price8,
      previousClose: Price8,
      tradedVol: Qty,
      tradedValue: Price8,
      change: Price8,
      changePercent: Int,
      tradeTs: Long,
      decimalsInPrice: Short,
      marketTs: Nano,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      klk <- EitherT.rightT[Task, AppError](keyMarketStats(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          klk,
          Map(
            this.o             -> Store.longToBigDecimal(o.value, decimalsInPrice).toString,
            this.h             -> Store.longToBigDecimal(h.value, decimalsInPrice).toString,
            this.l             -> Store.longToBigDecimal(l.value, decimalsInPrice).toString,
            this.c             -> Store.longToBigDecimal(c.value, decimalsInPrice).toString,
            this.previousClose -> Store.longToBigDecimal(previousClose.value, decimalsInPrice).toString,
            this.tradedVolume  -> tradedVol.value.toString,
            this.tradeValue    -> tradedValue.value.toString,
            this.change        -> Store.longToBigDecimal(change.value, decimalsInPrice).toString,
            this.changePercent -> Store.intToBigDecimal(changePercent, decimalsInPrice).toString,
            this.tss           -> marketTs.value,
            this.tsb           -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  override def updateMySqlIPOPrice(
      oid: OrderbookId,
      ipoPrice: Price,
      decimalsInPrice: Short
  ): Task[Either[AppError, Unit]] =
    ().asRight.pure[Task]

  override def updateMySqlSettlementPrice(
      oid: OrderbookId,
      marketTs: Nano,
      settlPrice: Price,
      decimalsInPrice: Short
  ): Task[Either[AppError, Unit]] =
    ().asRight.pure[Task] // UNUSED

  override def saveDecimalsInPrice(oid: OrderbookId, d: Short): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](cacheDecimalsInPrice += oid -> d)
      _ <- EitherT.rightT[Task, AppError](commands.get.hset(keyDecimalsInPrice, oid.value.toString, d.toString))
    } yield ()).value

  override def getDecimalsInPrice(oid: OrderbookId): Task[Either[AppError, Short]] =
    (for {
      res <- EitherT.fromEither[Task](cacheDecimalsInPrice.get(oid).fold(1.asInstanceOf[Short].asRight)(p => p.asRight))
    } yield res).value

  val priceType = "price_type"
  val price     = "price"
  override def updateReferencePrice(
      oid: OrderbookId,
      symbol: Instrument,
      priceType: Byte,
      price: Price,
      decimalsInPrice: Short,
      marketTs: Nano,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      klk <- EitherT.rightT[Task, AppError](keyReferencePrice(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          klk,
          Map(
            this.priceType -> priceType.toString,
            this.price     -> Store.longToBigDecimal(price.value, decimalsInPrice).toString,
            this.tss       -> marketTs.value,
            this.tsb       -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value
}
