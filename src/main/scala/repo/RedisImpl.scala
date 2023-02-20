package com.guardian
package repo

import AppError.{RedisConnectionError, SecondNotFound, SymbolNotFound}
import Config.Channel
import entity.{Instrument, Micro, OrderbookId, Price, Qty}

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import io.lettuce.core.{Limit, Range, RedisClient}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import monix.eval.Task

import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava, MapHasAsScala}
import scala.util.{Failure, Success, Try}

class RedisImpl(channel: Channel, client: RedisClient) extends Store(channel) {
  private var connection: Option[StatefulRedisConnection[String, String]] = Option.empty
  private var commands: Option[RedisCommands[String, String]]             = Option.empty

  override def connect: Task[Either[AppError, Unit]] = {
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

  override def saveSymbolByOrderbookId(oid: OrderbookId, symbol: Instrument): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT.rightT[Task, AppError](commands.get.hset(symbolReferenceTable, oid.value.toString, symbol.value))
    } yield ()).value

  override def getSymbol(oid: OrderbookId): Task[Either[AppError, Instrument]] =
    (for {
      res <- EitherT.fromEither[Task](
        Option(commands.get.hget(symbolReferenceTable, oid.value.toString))
          .fold(SymbolNotFound(oid).asLeft[Instrument])(p => Instrument(p).asRight)
      )
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

  override def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]] =
    (for {
      obk <- EitherT.rightT[Task, AppError](keyOrderbook(symbol))
      res <- EitherT.rightT[Task, AppError](
        commands.get
          .xrevrange(obk, Range.create("+", "-"), Limit.create(0, 1))
          .asScala
          .headOption
          .map(p => {
            val body      = p.getBody.asScala
            val seq       = body.getOrElse(id, "-1").toLong
            val maxLevel  = body.getOrElse(this.maxLevel, "10").toInt
            val marketTs  = Micro(body.getOrElse(this.tss, "0").toLong)
            val bananaTs  = Micro(body.getOrElse(this.tsb, "0").toLong)
            val askPrices = body.getOrElse(a, "").split(",").map(p => Price(p.toInt)).padTo(maxLevel, Price(0))
            val bidPrices = body.getOrElse(b, "").split(",").map(p => Price(p.toInt)).padTo(maxLevel, Price(0))
            val askQtys   = body.getOrElse(aq, "").split(",").map(p => Qty(p.toLong)).padTo(maxLevel, Qty(0L))
            val bidQtys   = body.getOrElse(bq, "").split(",").map(p => Qty(p.toLong)).padTo(maxLevel, Qty(0L))
            val askTimes  = body.getOrElse(at, "").split(",").map(p => Micro(p.toLong)).padTo(maxLevel, Micro(0L))
            val bidTimes  = body.getOrElse(bt, "").split(",").map(p => Micro(p.toLong)).padTo(maxLevel, Micro(0L))
            val asks = askPrices.zip(askQtys).zip(askTimes).map {
              case ((px, qt), _) if px.value == 0 && qt.value == 0L => None
              case ((px, qt), t)                                    => Some((px, qt, t))
            }
            val bids = bidPrices.zip(bidQtys).zip(bidTimes).map {
              case ((px, qt), _) if px.value == 0 && qt.value == 0L => None
              case ((px, qt), t)                                    => Some((px, qt, t))
            }
            OrderbookItem(seq, maxLevel, bids, asks, marketTs = marketTs, bananaTs = bananaTs)
          })
      )
    } yield res).value

  override def saveOrderbookItem(symbol: Instrument, orderbookId: OrderbookId, item: OrderbookItem): Task[Either[AppError, Unit]] =
    (for {
      obk <- EitherT.rightT[Task, AppError](keyOrderbook(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        val id       = item.seq
        val maxLevel = item.maxLevel
        val aTup = item.asks.map {
          case Some(value) => (value._1, value._2, value._3)
          case None        => (Price(0), Qty(0L), Micro(0L))
        }
        val a  = s"""[${aTup.map(_._1.value).mkString(",")}]"""
        val aq = s"""[${aTup.map(_._2.value).mkString(",")}]"""
        val at = s"""[${aTup.map(_._3.value).mkString(",")}]"""
        val bTup = item.asks.map {
          case Some(value) => (value._1, value._2, value._3)
          case None        => (Price(0), Qty(0L), Micro(0L))
        }
        val b  = s"""[${bTup.map(_._1.value).mkString(",")}]"""
        val bq = s"""[${bTup.map(_._2.value).mkString(",")}]"""
        val bt = s"""[${bTup.map(_._3.value).mkString(",")}]"""
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
            this.bt       -> bt
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
          .xrevrange(tck, Range.create("+", "-"), Limit.create(0, 1))
          .asScala
          .headOption
          .map(p => {
            val body = p.getBody.asScala
            Qty(body(this.q).toLong)
          })
          .getOrElse(Qty(0L))
      )
    } yield res).value

  override def saveTicker(
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      aggressor: Byte,
      tq: Qty,
      dealSource: Byte,
      action: Byte,
      tradeReportCode: Short,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      tck <- EitherT.rightT[Task, AppError](keyOrderbook(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          tck,
          Map(
            this.id              -> seq.toString,
            this.p               -> p.value.toString,
            this.q               -> q.value.toString,
            this.s               -> aggressor.asInstanceOf[Char].toString,
            this.tq              -> tq.value.toString,
            this.action          -> action.asInstanceOf[Char].toString,
            this.tradeReportCode -> tradeReportCode.toString,
            this.dealSource      -> dealSource.asInstanceOf[Char].toString,
            this.tss             -> marketTs.value.toString,
            this.tsb             -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value

  override def updateTicker(
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      aggressor: Byte,
      dealSource: Byte,
      action: Byte,
      tradeReportCode: Short,
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
          symbol = symbol,
          seq = seq,
          p = p,
          q = q,
          tq = newTq,
          aggressor = aggressor,
          dealSource = dealSource,
          action = action,
          tradeReportCode = tradeReportCode,
          marketTs = marketTs,
          bananaTs = bananaTs
        )
      )
    } yield ()).value

  val ib = "ib"
  override def updateProjected(
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
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
            this.p   -> p.value.toString,
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
      symbol: Instrument,
      seq: Long,
      o: Price,
      h: Price,
      l: Price,
      c: Price,
      lastAuctionPx: Price,
      avgpx: Price,
      turnOverQty: Qty,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      klk <- EitherT.rightT[Task, AppError](keyKlein(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          klk,
          Map(
            this.o             -> o.value.toString,
            this.h             -> h.value.toString,
            this.l             -> l.value.toString,
            this.c             -> c.value.toString,
            this.lastAuctionPx -> lastAuctionPx.value.toString,
            this.avgpx         -> avgpx.value.toString,
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
      symbol: Instrument,
      seq: Long,
      o: Qty,
      h: Qty,
      l: Qty,
      c: Qty,
      previousClose: Qty,
      tradedVol: Qty,
      tradedValue: Qty,
      change: Long,
      changePercent: Int,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      klk <- EitherT.rightT[Task, AppError](keyMarketStats(symbol))
      _ <- EitherT.rightT[Task, AppError] {
        commands.get.xadd(
          klk,
          Map(
            this.o             -> o.value.toString,
            this.h             -> h.value.toString,
            this.l             -> l.value.toString,
            this.c             -> c.value.toString,
            this.previousClose -> previousClose.value.toString,
            this.tradedVolume  -> tradedVol.value.toString,
            this.tradeValue    -> tradedValue.value.toString,
            this.change        -> change.toString,
            this.changePercent -> changePercent.toString,
            this.tss           -> marketTs.value.toString,
            this.tsb           -> bananaTs.value.toString
          ).asJava
        )
      }
    } yield ()).value
}
