package com.guardian
package repo

import AppError.{RedisConnectionError, SecondNotFound, SymbolNotFound}
import Config.{Channel, DbType}
import entity._

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{Limit, Range, RedisClient}
import monix.eval.Task

import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, MapHasAsScala}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class RedisImpl(channel: Channel, client: RedisClient) extends Store(channel, DbType.redis) {
  private var connection: Option[StatefulRedisConnection[String, String]] = Option.empty
  private var commands: Option[RedisCommands[String, String]]             = Option.empty

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
      _ <- EitherT.rightT[Task, AppError](
        Option(commands.get.hset(keySecond, this.value, unixSecond.toString))
      )
    } yield ()).value

  override def getSecond: Task[Either[AppError, Int]] =
    (for {
      s <- EitherT.fromEither[Task](
        Option(commands.get.hget(keySecond, this.value)).fold(SecondNotFound.asLeft[Int])(p => p.toInt.asRight)
      )
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
      marketTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](commands.get.hset(keyTradableInstrument, oid.value.toString, symbol.value))
    } yield ()).value

  override def getInstrument(oid: OrderbookId): Task[Either[AppError, Instrument]] =
    (for {
      res <- EitherT.fromEither[Task](
        Option(commands.get.hget(keyTradableInstrument, oid.value.toString))
          .fold(SymbolNotFound(oid).asLeft[Instrument])(p => Instrument(p).asRight)
      )
    } yield res).value

  private val id                    = "id"
  private val a                     = "a"
  private val b                     = "b"
  private val aq                    = "aq"
  private val bq                    = "bq"
  private val at                    = "at"
  private val bt                    = "bt"
  private val tss                   = "tss"
  private val tsb                   = "tsb"
  private val maxLevel              = "maxLevel"
  private val bracketPattern: Regex = "(?<=\\[).+?(?=])".r

  private val mapToPrices: (mutable.Map[String, String], String, Short) => Array[Price] = (map, key, decInPrice) =>
    map
      .get(key)
      .flatMap(p => bracketPattern findFirstIn p)
      .getOrElse("")
      .split(",")
      .flatMap(p => Option(p).filter(_.trim.nonEmpty))
      .map(p => Price(Store.bigDecimalToInt(p.toFloat, decInPrice)))

  private val mapToQty: (mutable.Map[String, String], String) => Array[Qty] = (map, key) =>
    map
      .get(key)
      .flatMap(p => bracketPattern findFirstIn p)
      .getOrElse("")
      .split(",")
      .flatMap(p => Option(p).filter(_.trim.nonEmpty))
      .map(p => Qty(p.toLong))

  private val mapToMicro: (mutable.Map[String, String], String) => Array[Micro] = (map, key) =>
    map
      .get(key)
      .flatMap(p => bracketPattern findFirstIn p)
      .getOrElse("")
      .split(",")
      .flatMap(p => Option(p).filter(_.trim.nonEmpty))
      .map(p => Micro(p.toLong))

  override def getLastOrderbookItem(
      symbol: Instrument,
      decimalsInPrice: Short
  ): Task[Either[AppError, Option[OrderbookItem]]] =
    (for {
      obk <- EitherT.rightT[Task, AppError](keyOrderbook(symbol))
      res <- EitherT.rightT[Task, AppError](
        commands.get
          .xrevrange(obk, Range.create("-", "+"), Limit.create(0, 1))
          .asScala
          .headOption
          .map(p => {
            val body      = p.getBody.asScala
            val seq       = body.getOrElse(id, "-1").toLong
            val maxLevel  = body.getOrElse(this.maxLevel, "10").toInt
            val marketTs  = Micro(body.getOrElse(this.tss, "0").toLong)
            val bananaTs  = Micro(body.getOrElse(this.tsb, "0").toLong)
            val askPrices = mapToPrices(body, a, decimalsInPrice)
            val bidPrices = mapToPrices(body, b, decimalsInPrice)
            val askQtys   = mapToQty(body, aq)
            val bidQtys   = mapToQty(body, bq)
            val askTimes  = mapToMicro(body, at)
            val bidTimes  = mapToMicro(body, bt)
            val asks = askPrices
              .zip(askQtys)
              .zip(askTimes)
              .map {
                case ((px, qt), _) if px.value == 0 && qt.value == 0L => None
                case ((px, qt), t)                                    => Some((px, qt, t))
              }
              .toVector
            val bids = bidPrices
              .zip(bidQtys)
              .zip(bidTimes)
              .map {
                case ((px, qt), _) if px.value == 0 && qt.value == 0L => None
                case ((px, qt), t)                                    => Some((px, qt, t))
              }
              .toVector
            OrderbookItem(
              seq = seq,
              maxLevel = maxLevel,
              bids = bids,
              asks = asks,
              marketTs = marketTs,
              bananaTs = bananaTs
            )
          })
      )
    } yield res).value

  override def saveOrderbookItem(
      symbol: Instrument,
      orderbookId: OrderbookId,
      item: OrderbookItem,
      decimalsInPrice: Short
  ): Task[Either[AppError, Unit]] =
    (for {
      obk <- EitherT.rightT[Task, AppError](keyOrderbook(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        val id       = item.seq
        val maxLevel = item.maxLevel
        val aTup = item.asks.map {
          case Some(value) => (Store.intToBigDecimal(value._1.value, decimalsInPrice), value._2, value._3)
          case None        => (0.0f, Qty(0L), Micro(0L))
        }
        val a  = s"""[${aTup.map(_._1).mkString(",")}]"""
        val aq = s"""[${aTup.map(_._2.value).mkString(",")}]"""
        val at = s"""[${aTup.map(_._3.value).mkString(",")}]"""
        val bTup = item.bids.map {
          case Some(value) => (Store.intToBigDecimal(value._1.value, decimalsInPrice), value._2, value._3)
          case None        => (0.0f, Qty(0L), Micro(0L))
        }
        val b        = s"""[${bTup.map(_._1).mkString(",")}]"""
        val bq       = s"""[${bTup.map(_._2.value).mkString(",")}]"""
        val bt       = s"""[${bTup.map(_._3.value).mkString(",")}]"""
        val marketTs = item.marketTs.value.toString
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
      marketTs: Micro,
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
            this.action          -> action.toChar.toString,
            this.tradeReportCode -> tradeReportCode.toString,
            this.dealSource      -> dealSource.toChar.toString,
            this.tss             -> marketTs.value.toString,
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
      marketTs: Micro,
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
      marketTs: Micro,
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
            this.tss -> marketTs.value.toString,
            this.tsb -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  val o             = "o"
  val h             = "h"
  val l             = "l"
  val c             = "c"
  val lastAuctionPx = "lauctpx"
  val avgpx         = "avgpx"
  val turnOverQty   = "val"

  override def updateKline(
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
      decimalsInPrice: Short,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      klk <- EitherT.rightT[Task, AppError](keyKlein(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          klk,
          Map(
            this.o             -> Store.intToBigDecimal(o.value, decimalsInPrice).toString(),
            this.h             -> Store.intToBigDecimal(h.value, decimalsInPrice).toString(),
            this.l             -> Store.intToBigDecimal(l.value, decimalsInPrice).toString(),
            this.c             -> Store.intToBigDecimal(c.value, decimalsInPrice).toString(),
            this.lastAuctionPx -> Store.intToBigDecimal(lastAuctionPx.value, decimalsInPrice).toString(),
            this.avgpx         -> Store.intToBigDecimal(avgpx.value, decimalsInPrice).toString(),
            this.turnOverQty   -> turnOverQty.value.toString,
            this.tss           -> marketTs.value.toString,
            this.tsb           -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  private val previousClose = "pc"
  private val tradedVolume  = "vol"
  private val tradeValue    = "val"
  private val change        = "change"
  private val changePercent = "changePercent"
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
      marketTs: Micro,
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
            this.change        -> change.value.toString,
            this.changePercent -> changePercent.toString,
            this.tss           -> marketTs.value.toString,
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
      marketTs: Micro,
      settlPrice: Price,
      decimalsInPrice: Short
  ): Task[Either[AppError, Unit]] =
    ().asRight.pure[Task] // UNUSED

  override def saveDecimalsInPrice(oid: OrderbookId, d: Short): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](commands.get.hset(keyDecimalsInPrice, oid.value.toString, d.toString))
    } yield ()).value

  override def getDecimalsInPrice(oid: OrderbookId): Task[Either[AppError, Short]] =
    (for {
      res <- EitherT.fromEither[Task](
        Option(commands.get.hget(keyDecimalsInPrice, oid.value.toString))
          .fold(1.asInstanceOf[Short].asRight)(p => p.toShort.asRight)
      )
    } yield res).value
}
