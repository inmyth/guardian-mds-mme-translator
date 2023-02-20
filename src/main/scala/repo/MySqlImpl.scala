package com.guardian
package repo

import Config.Channel
import entity._

import cats.data.EitherT
import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.general.ArrayRowData
import monix.eval.Task

import java.time.{Instant, ZoneId}

import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.FutureConverters

class MySqlImpl(channel: Channel, connection: Connection) extends Store(channel) {
  private val zoneId: ZoneId           = ZoneId.of("Asia/Bangkok")
  val MDS_DerivativeOrderBook          = "MDS_DerivativeOrderBook"
  val MDS_EquityOrderBook              = "MDS_EquityOrderBook"
  val MDS_EquityTradableInstrument     = "MDS_EquityTradableInstrument"
  val MDS_DerivativeTradableInstrument = "MDS_DerivativeTradableInstrument"

  val (maxLevel, orderbookTable, tradableInstrumentTable) = channel match {
    case Channel.eq => (10, MDS_EquityOrderBook, MDS_EquityTradableInstrument)
    case Channel.fu => (5, MDS_DerivativeOrderBook, MDS_DerivativeTradableInstrument)
  }

  val orderTable     = "orders"
  val executionTable = "executions"
  val errorTable     = "errors"
  val cExecutionId   = "execution_id"
  val cUserOrderId   = "user_order_id"
  val cBrokerOrderId = "broker_order_id"
  val cMarketOrderId = "market_order_id"
  val cPortfolio     = "portfolio"
  val cUserId        = "user_id"     // option
  val cSymbol        = "symbol"
  val cMarket        = "market"
  val cQuantity      = "quantity"    // option
  val cPrice         = "price"       // option
  val cSide          = "side"        // option
  val cOrderType     = "order_type"  // option
  val cExecStatus    = "exec_status"
  val cLeaves        = "leaves"
  val cFilled        = "filled"
  val cAvgFill       = "avg_fill"    // option
  val cOrderMicros   = "order_micros"
  val cDealMicros    = "deal_micros" // option
  val cTime          = "time"
  val cError         = "error"

  val cOrderBookUpdateTime                = "UpdateTime"
  val cOrderBookSourceTime                = "SourceTime"
  val cOrderBookReceivingTime             = "ReceivingTime"
  val cOrderBookSeqNo                     = "SeqNo"
  val cOrderBookSecCode                   = "SecCode"
  val cOrderBookSecName                   = "SecName"
  val cOrderBookMDBidPrice: Int => String = (i: Int) => s"MDBid${i}Price"
  val cOrderBookMDBidSize: Int => String  = (i: Int) => s"MDBid${i}Size"
  val cOrderBookMDAskPrice: Int => String = (i: Int) => s"MDAsk${i}Price"
  val cOrderBookMDAskSize: Int => String  = (i: Int) => s"MDAsk${i}Size"

  override def connect: Task[Either[AppError, Unit]] = ???

  override def disconnect: Task[Either[AppError, Unit]] = ???

  override def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]] = ???

  override def getSecond: Task[Either[AppError, Int]] = ???

  override def saveSymbolByOrderbookId(oid: OrderbookId, symbol: Instrument): Task[Either[AppError, Unit]] = ???

  override def getSymbol(orderbookId: OrderbookId): Task[Either[AppError, Instrument]] = ???

  val cSeqNo         = "SeqNo"
  val cUpdateTime    = "UpdateTime"
  val cSourceTime    = "SourceTime"
  val cReceivingTime = "ReceivingTime"
  val cSecName       = "SecName"
  val cSecCode       = "SecCode"

  override def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]] =
    (for {
      query <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT * FROM $orderbookTable WHERE $cSourceTime=(SELECT max($cSourceTime) FROM $orderbookTable);
         |""".stripMargin
      )
      data <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(query))))
      res <- EitherT.rightT[Task, AppError](
        data.getRows
          .stream()
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => {
            val seq      = p.getLong(cSeqNo)
            val marketTs = Micro(p.getDate(cSourceTime).atZone(zoneId).toInstant.toEpochMilli * 1000)
            val bananaTs = Micro(p.getDate(cReceivingTime).atZone(zoneId).toInstant.toEpochMilli * 1000)
            val asks = (1 to maxLevel).map(i => {
              val maybePx  = Option(p.getInt(cOrderBookMDAskPrice(i)))
              val maybeQty = Option(p.getLong(cOrderBookMDAskSize(i)))
              (maybePx, maybeQty) match {
                case (None, None) => None
                case _            => Some(Price(maybePx.fold(0)(p => p)), Qty(maybeQty.fold(0L)(p => p)), Micro(0L))
              }
            })
            val bids = (1 to maxLevel).map(i => {
              val maybePx  = Option(p.getInt(cOrderBookMDBidPrice(i)))
              val maybeQty = Option(p.getLong(cOrderBookMDBidSize(i)))
              (maybePx, maybeQty) match {
                case (None, None) => None
                case _            => Some(Price(maybePx.fold(0)(p => p)), Qty(maybeQty.fold(0L)(p => p)), Micro(0L))
              }
            })
            OrderbookItem(
              seq = seq,
              maxLevel = maxLevel,
              bids = bids,
              asks = asks,
              marketTs = marketTs,
              bananaTs = bananaTs
            )
          })
          .headOption
      )
    } yield res).value

  override def saveOrderbookItem(
      symbol: Instrument,
      orderbookId: OrderbookId,
      item: OrderbookItem
  ): Task[Either[AppError, Unit]] =
    (for {
      asks  <- EitherT.rightT[Task, AppError](item.asks.filter(_.isDefined).map(_.get))
      bids  <- EitherT.rightT[Task, AppError](item.bids.filter(_.isDefined).map(_.get))
      table <- EitherT.rightT[Task, AppError](if (item.maxLevel == 10) MDS_EquityOrderBook else MDS_DerivativeOrderBook)
      query <- EitherT.rightT[Task, AppError](
        s"""
         |INSERT INTO $table
         |($cUpdateTime, $cSourceTime, $cReceivingTime, $cSeqNo, $cSecCode, $cSecName,
         |${(1 to bids.size).map(p => cOrderBookMDBidPrice(p)).mkString(", ")},
         |${(1 to bids.size).map(p => cOrderBookMDBidSize(p)).mkString(", ")},
         |${(1 to asks.size).map(p => cOrderBookMDAskPrice(p)).mkString(", ")},
         |${(1 to asks.size).map(p => cOrderBookMDAskSize(p)).mkString(", ")})
         |VALUES
         |(?,?,?,?,?,?,
         |${(1 to 2 * bids.size).map(_ => "?").mkString(",")},
         |${(1 to 2 * asks.size).map(_ => "?").mkString(",")}
         |)
         |""".stripMargin
      )
      params <- EitherT.rightT[Task, AppError] {
        val sourceTime =
          Instant.ofEpochMilli(item.marketTs.value / 1000).atZone(ZoneId.systemDefault()).toLocalDate.toString
        val receivingTime =
          Instant.ofEpochMilli(item.bananaTs.value / 1000).atZone(ZoneId.systemDefault()).toLocalDate.toString
        Vector(sourceTime, sourceTime, receivingTime, item.seq, orderbookId.value, symbol.value) ++
          bids.map(_._1.value) ++ bids.map(_._2.value) ++
          asks.map(_._1.value) ++ asks.map(_._2.value)
      }
      _ <- EitherT.rightT[Task, AppError](connection.sendPreparedStatement(query, params.asJava))
    } yield ()).value

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
