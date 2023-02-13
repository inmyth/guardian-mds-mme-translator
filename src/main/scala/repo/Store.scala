package com.guardian
package repo

import AppError.{RedisConnectionError, SecondNotFound, SymbolNotFound}
import Config.{Channel, RedisConfig}
import entity._
import repo.InMemImpl.{KlineItem, MarketStatsItem, ProjectedItem, TickerItem}

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{Limit, Range, RedisClient}
import monix.eval.Task

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

abstract class Store(channel: Channel) {
  val symbolReferenceTable                 = s"${channel.toString}:symbol_reference"
  val keySecond                            = s"${channel.toString}:second"
  val keyOrderbook: Instrument => String   = (symbol: Instrument) => s"${channel.toString}:orderbook:${symbol.value}"
  val keyTicker: Instrument => String      = (symbol: Instrument) => s"${channel.toString}:tick:${symbol.value}"
  val keyProjected: Instrument => String   = (symbol: Instrument) => s"${channel.toString}:projected:${symbol.value}"
  val keyKlein: Instrument => String       = (symbol: Instrument) => s"${channel.toString}:klein:${symbol.value}"
  val keyMarketStats: Instrument => String = (symbol: Instrument) => s"id:mkt:${symbol.value}"

  def connect: Task[Either[AppError, Unit]]
  def disconnect: Task[Either[AppError, Unit]]

  def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]]
  def getSecond: Task[Either[AppError, Int]]
  def saveSymbolByOrderbookId(oid: OrderbookId, symbol: Instrument): Task[Either[AppError, Unit]]
  def getSymbol(orderbookId: OrderbookId): Task[Either[AppError, Instrument]]

  def updateOrderbook(seq: Long, item: FlatPriceLevelAction): Task[Either[AppError, Unit]]
  def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]]
  def saveOrderbookItem(symbol: Instrument, item: OrderbookItem): Task[Either[AppError, Unit]]
  def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]]
  def saveTicker(
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
  ): Task[Either[AppError, Unit]]
  def updateTicker(
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
  ): Task[Either[AppError, Unit]]
  def updateProjected(
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]]
  def updateKline(
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
  ): Task[Either[AppError, Unit]]
  def updateMarketStats(
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
  ): Task[Either[AppError, Unit]]
}

class InMemImpl(channel: Channel) extends Store(channel) {

  private var marketSecondDb: Option[Int]                         = None
  private var symbolReferenceDb: Map[OrderbookId, Instrument]     = Map.empty
  private var orderbookDb: Map[String, Vector[OrderbookItem]]     = Map.empty
  private var tickerDb: Map[String, Vector[TickerItem]]           = Map.empty
  private var projectedDb: Map[String, Vector[ProjectedItem]]     = Map.empty
  var klineDb: Map[String, Vector[KlineItem]]                     = Map.empty
  private var marketStatsDb: Map[String, Vector[MarketStatsItem]] = Map.empty
  override def connect: Task[Either[AppError, Unit]]              = ().asRight.pure[Task]

  override def disconnect: Task[Either[AppError, Unit]] = ().asRight.pure[Task]

  override def saveSymbolByOrderbookId(oid: OrderbookId, symbol: Instrument): Task[Either[AppError, Unit]] = {
    symbolReferenceDb = symbolReferenceDb + (oid -> symbol)
    ().asRight.pure[Task]
  }

  override def getSymbol(oid: OrderbookId): Task[Either[AppError, Instrument]] =
    symbolReferenceDb.get(oid).fold(SymbolNotFound(oid).asLeft[Instrument])(p => p.asRight).pure[Task]

  override def saveOrderbookItem(symbol: Instrument, item: OrderbookItem): Task[Either[AppError, Unit]] = {
    val list = orderbookDb.getOrElse(symbol.value, Vector.empty)
    orderbookDb += (symbol.value -> (Vector(item) ++ list))
    ().asRight[AppError].pure[Task]
  }

  override def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]] =
    orderbookDb.getOrElse(symbol.value, Vector.empty).sortWith(_.seq > _.seq).headOption.asRight.pure[Task]

  override def updateOrderbook(seq: Long, item: FlatPriceLevelAction): Task[Either[AppError, Unit]] =
    (for {
      mayb <- EitherT(getLastOrderbookItem(item.symbol))
      last <-
        if (mayb.isDefined) {
          EitherT.rightT[Task, AppError](mayb.get)
        }
        else {
          EitherT.rightT[Task, AppError](OrderbookItem.empty(item.maxLevel))
        }
      nu <- EitherT.rightT[Task, AppError](item.levelUpdateAction.asInstanceOf[Char] match {
        case 'N' => last.insert(item.price, item.qty, item.marketTs, item.level, item.side)
        case 'D' => last.delete(side = item.side, level = item.level, numDeletes = item.numDeletes)
        case _ =>
          last
            .update(side = item.side, level = item.level, price = item.price, qty = item.qty, marketTs = item.marketTs)
      })
      up <- EitherT.rightT[Task, AppError](
        OrderbookItem.reconstruct(
          nu,
          side = item.side.value,
          seq = seq,
          origin = last,
          marketTs = item.marketTs,
          maxLevel = item.maxLevel,
          bananaTs = item.bananaTs
        )
      )
      _ <- EitherT(saveOrderbookItem(item.symbol, up))
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
  override def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]] = {
    marketSecondDb = Some(unixSecond)
    ().asRight.pure[Task]
  }

  override def getSecond: Task[Either[AppError, Int]] =
    marketSecondDb.fold(SecondNotFound.asLeft[Int])(p => p.asRight).pure[Task]

  override def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]] =
    tickerDb
      .getOrElse(symbol.value, Vector.empty)
      .sortWith(_.seq > _.seq)
      .headOption
      .map(_.tq)
      .getOrElse(Qty(0L))
      .asRight
      .pure[Task]

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
  ): Task[Either[AppError, Unit]] = {
    val list = tickerDb.getOrElse(symbol.value, Vector.empty)
    tickerDb += (symbol.value -> (Vector(
      TickerItem(
        seq = seq,
        p = p,
        q = q,
        aggressor = aggressor,
        tq = tq,
        dealSource = dealSource,
        action = action,
        tradeReportCode = tradeReportCode,
        marketTs = marketTs,
        bananaTs = bananaTs
      )
    ) ++ list))
    ().asRight[AppError].pure[Task]
  }

  override def updateProjected(
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] = {
    val list = projectedDb.getOrElse(symbol.value, Vector.empty)
    projectedDb += (symbol.value -> (Vector(
      ProjectedItem(
        seq = seq,
        p = p,
        q = q,
        ib = ib,
        marketTs = marketTs,
        bananaTs = bananaTs
      )
    ) ++ list))
    ().asRight[AppError].pure[Task]
  }

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
  ): Task[Either[AppError, Unit]] = {
    val list = klineDb.getOrElse(symbol.value, Vector.empty)
    klineDb += (symbol.value -> (Vector(
      KlineItem(
        seq = seq,
        o = o,
        h = h,
        l = l,
        c = c,
        lauctpx = lastAuctionPx,
        turnOverQty = turnOverQty,
        avgpx = avgpx,
        marketTs = marketTs,
        bananaTs = bananaTs
      )
    ) ++ list))
    ().asRight[AppError].pure[Task]
  }

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
  ): Task[Either[AppError, Unit]] = {
    val list = marketStatsDb.getOrElse(symbol.value, Vector.empty)
    marketStatsDb += (symbol.value -> (Vector(
      MarketStatsItem(
        seq = seq,
        o = o,
        h = h,
        l = l,
        c = c,
        previousClose = previousClose,
        tradedVol = tradedVol,
        tradedValue = tradedValue,
        change = change,
        changePercent = changePercent,
        marketTs = marketTs,
        bananaTs = bananaTs
      )
    ) ++ list))
    ().asRight[AppError].pure[Task]
  }
}

object InMemImpl {
  case class TickerItem(
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
  )
  case class ProjectedItem(
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
      marketTs: Micro,
      bananaTs: Micro
  )

  case class KlineItem(
      seq: Long,
      o: Price,
      h: Price,
      l: Price,
      c: Price,
      lauctpx: Price,
      avgpx: Price,
      turnOverQty: Qty,
      marketTs: Micro,
      bananaTs: Micro
  )
  case class MarketStatsItem(
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
  )
}

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

  override def updateOrderbook(seq: Long, item: FlatPriceLevelAction): Task[Either[AppError, Unit]] =
    (for {
      maybeItem <- EitherT(getLastOrderbookItem(item.symbol))
      lastItem <-
        if (maybeItem.isDefined) {
          EitherT.rightT[Task, AppError](maybeItem.get)
        }
        else {
          EitherT.rightT[Task, AppError](OrderbookItem.empty(item.maxLevel))
        }
      nuPriceLevels <- EitherT.rightT[Task, AppError](item.levelUpdateAction.asInstanceOf[Char] match {
        case 'N' => lastItem.insert(item.price, item.qty, item.marketTs, item.level, item.side)
        case 'D' => lastItem.delete(side = item.side, level = item.level, numDeletes = item.numDeletes)
        case _ =>
          lastItem
            .update(side = item.side, level = item.level, price = item.price, qty = item.qty, marketTs = item.bananaTs)
      })
      update <- EitherT.rightT[Task, AppError](
        OrderbookItem.reconstruct(
          nuPriceLevels,
          side = item.side.value,
          seq = seq,
          maxLevel = item.maxLevel,
          marketTs = item.marketTs,
          bananaTs = item.bananaTs,
          origin = lastItem
        )
      )
      _ <- EitherT(saveOrderbookItem(item.symbol, update))
    } yield ()).value

  override def saveOrderbookItem(symbol: Instrument, item: OrderbookItem): Task[Either[AppError, Unit]] =
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
            this.id       -> id,
            this.maxLevel -> maxLevel,
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
            this.id              -> seq,
            this.p               -> p.value.toString,
            this.q               -> q.value.toString,
            this.s               -> aggressor.asInstanceOf[Char],
            this.tq              -> tq.value.toString,
            this.action          -> action.asInstanceOf[Char],
            this.tradeReportCode -> tradeReportCode.toString,
            this.dealSource      -> dealSource.asInstanceOf[Char],
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
            this.id  -> seq,
            this.p   -> p.value.toString,
            this.q   -> q.value.toString,
            this.ib  -> ib.value,
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
            this.o             -> o.value,
            this.h             -> h.value,
            this.l             -> l.value,
            this.c             -> c.value,
            this.lastAuctionPx -> lastAuctionPx.value,
            this.avgpx         -> avgpx.value,
            this.turnOverQty   -> turnOverQty.value,
            this.tss           -> marketTs.value,
            this.tsb           -> bananaTs.value
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
            this.o             -> o.value,
            this.h             -> h.value,
            this.l             -> l.value,
            this.c             -> c.value,
            this.previousClose -> previousClose.value,
            this.tradedVolume  -> tradedVol.value,
            this.tradeValue    -> tradedValue.value,
            this.change        -> change,
            this.changePercent -> changePercent,
            this.tss           -> marketTs.value,
            this.tsb           -> bananaTs.value
          ).asJava
        )
      }
    } yield ()).value
}

class MySqlImpl(channel: Channel) extends Store(channel) {
  override def connect: Task[Either[AppError, Unit]] = ???

  override def disconnect: Task[Either[AppError, Unit]] = ???

  override def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]] = ???

  override def getSecond: Task[Either[AppError, Int]] = ???

  override def saveSymbolByOrderbookId(oid: OrderbookId, symbol: Instrument): Task[Either[AppError, Unit]] = ???

  override def getSymbol(orderbookId: OrderbookId): Task[Either[AppError, Instrument]] = ???

  override def updateOrderbook(seq: Long, item: FlatPriceLevelAction): Task[Either[AppError, Unit]] = ???

  override def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]] = ???

  override def saveOrderbookItem(symbol: Instrument, item: OrderbookItem): Task[Either[AppError, Unit]] = ???

  override def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]] = ???

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
  ): Task[Either[AppError, Unit]] = ???

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
  ): Task[Either[AppError, Unit]] = ???

  override def updateProjected(
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] = ???

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
  ): Task[Either[AppError, Unit]] = ???

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
  ): Task[Either[AppError, Unit]] = ???
}

object Store {

  def redis(channel: Channel, redisConfig: RedisConfig) =
    new RedisImpl(channel, RedisClient.create(s"redis://${redisConfig.host}:${redisConfig.port}"))

  def mysql(channel: Channel) = new MySqlImpl(channel)
}
