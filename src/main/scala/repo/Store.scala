package com.guardian
package repo

import AppError.{SecondNotFound, SymbolNotFound}
import Config.{Channel, MySqlConfig, RedisConfig}
import entity._
import repo.InMemImpl.{KlineItem, MarketStatsItem, ProjectedItem, TickerItem}

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import com.github.jasync.sql.db.mysql.MySQLConnectionBuilder
import io.lettuce.core.RedisClient
import monix.eval.Task

abstract class Store(channel: Channel) {
  val keyTradableInstrument              = s"${channel.toString}:symbol_reference"
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

  def saveTradableInstrument(
      oid: OrderbookId,
      symbol: Instrument,
      secType: String,
      secDesc: String,
      allowShortSell: Byte,
      allowNVDR: Byte,
      allowShortSellOnNVDR: Byte,
      allowTTF: Byte,
      isValidForTrading: Byte,
      isOddLot: Int,
      parValue: Long,
      sectorNumber: String,
      underlyingSecCode: Int,
      underlyingSecName: String,
      maturityDate: Int,
      contractMultiplier: Int,
      settlMethod: String
  ): Task[Either[AppError, Unit]]
  def getInstrument(orderbookId: OrderbookId): Task[Either[AppError, Instrument]]

  def updateOrderbook(seq: Long, orderbookId: OrderbookId, item: FlatPriceLevelAction): Task[Either[AppError, Unit]] =
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
            .update(side = item.side, level = item.level, price = item.price, qty = item.qty, marketTs = item.marketTs)
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
      _ <- EitherT(saveOrderbookItem(item.symbol, orderbookId, update))
    } yield ()).value

  def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]]

  def saveOrderbookItem(symbol: Instrument, orderbookId: OrderbookId, item: OrderbookItem): Task[Either[AppError, Unit]]

  def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]]

  def saveTicker(
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
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]]

  def updateTicker(
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
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]]

  def updateProjected(
      oid: OrderbookId,
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
      tradeTs: Long,
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
      isOddLot: Int,
      parValue: Long,
      sectorNumber: String,
      underlyingSecCode: Int,
      underlyingSecName: String,
      maturityDate: Int,
      contractMultiplier: Int,
      settlMethod: String
  ): Task[Either[AppError, Unit]] = {
    symbolReferenceDb = symbolReferenceDb + (oid -> symbol)
    ().asRight.pure[Task]
  }

  override def getInstrument(oid: OrderbookId): Task[Either[AppError, Instrument]] =
    symbolReferenceDb.get(oid).fold(SymbolNotFound(oid).asLeft[Instrument])(p => p.asRight).pure[Task]

  override def saveOrderbookItem(
      symbol: Instrument,
      orderbookId: OrderbookId,
      item: OrderbookItem
  ): Task[Either[AppError, Unit]] = {
    val list = orderbookDb.getOrElse(symbol.value, Vector.empty)
    orderbookDb += (symbol.value -> (Vector(item) ++ list))
    ().asRight[AppError].pure[Task]
  }

  override def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]] =
    orderbookDb.getOrElse(symbol.value, Vector.empty).sortWith(_.seq > _.seq).headOption.asRight.pure[Task]

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
      oid: OrderbookId,
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
      tradeTs: Long,
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
        tradeTs = tradeTs,
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
      tradeTs: Long,
      marketTs: Micro,
      bananaTs: Micro
  )
}

object Store {

  def redis(channel: Channel, redisConfig: RedisConfig): Store =
    new RedisImpl(channel, RedisClient.create(s"redis://${redisConfig.host}:${redisConfig.port}"))

  def mysql(channel: Channel, mySqlConfig: MySqlConfig): Store = {
    val connection =
      MySQLConnectionBuilder.createConnectionPool(s"jdbc:mysql://${mySqlConfig.host}:${mySqlConfig.port}/mdsdb")
    new MySqlImpl(channel, connection)
  }
}
