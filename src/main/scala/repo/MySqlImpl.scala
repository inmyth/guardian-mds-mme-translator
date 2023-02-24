package com.guardian
package repo

import Config.Channel
import entity.{Price, _}

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.general.ArrayRowData
import com.guardian.AppError.SecondNotFound
import monix.eval.Task

import java.time.{Instant, ZoneId}

import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.FutureConverters

class MySqlImpl(channel: Channel, connection: Connection) extends Store(channel) {
  private val zoneId: ZoneId              = ZoneId.of("Asia/Bangkok")
  private var marketSecondDb: Option[Int] = None

  val (maxLevel, orderbookTable, tickerTable, dayTable, projectedTable, tradableInstrumentTable) = {
    val (ml, which) = channel match {
      case Channel.eq => (10, "Equity")
      case Channel.fu => (5, "Derivative")
    }
    (
      ml,
      s"MDS_${which}Orderbook",
      s"MDS_${which}Ticker",
      s"MDS_${which}Day",
      s"MDS_${which}ProjectedPrice",
      s"MDS_${which}InstrumentTable"
    )
  }

  val cOrderBookMDBidPrice: Int => String = (i: Int) => s"MDBid${i}Price"
  val cOrderBookMDBidSize: Int => String  = (i: Int) => s"MDBid${i}Size"
  val cOrderBookMDAskPrice: Int => String = (i: Int) => s"MDAsk${i}Price"
  val cOrderBookMDAskSize: Int => String  = (i: Int) => s"MDAsk${i}Size"
  val cSeqNo                              = "SeqNo"
  val cUpdateTime                         = "UpdateTime"
  val cSourceTime                         = "SourceTime"
  val cReceivingTime                      = "ReceivingTime"
  val cSecName                            = "SecName"
  val cSecCode                            = "SecCode"
  val cVolume                             = "Volume"
  val cSecType                            = "SecType"
  val cSecDesc                            = "SecDesc"
  val cProjTime                           = "ProjTime"
  val cProjPrice                          = "ProjPrice"
  val cProjVolume                         = "ProjVolume"
  val cProjImbalance                      = "ProjImbalance"
  val cisFinal                            = "IsFinal"
  val cTradeDate                          = "TradeDate"
  val cOpen1Price                         = "Open1Price"
  val cOpen2Price                         = "Open2Price"
  val cClose1Price                        = "Close1Price"
  val cClose2Price                        = "Close2Price"
  val cOpenPrice                          = "OpenPrice"
  val cClosePrice                         = "ClosePrice"
  val cSettlementPrice                    = "SettlementPrice"
  val cHighPrice                          = "HighPrice"
  val cLowPrice                           = "LowPrice"

  override def connect: Task[Either[AppError, Unit]] = ().asRight.pure[Task]

  override def disconnect: Task[Either[AppError, Unit]] = ().asRight.pure[Task]

  override def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]] = {
    marketSecondDb = Some(unixSecond)
    ().asRight.pure[Task]
  }

  override def getSecond: Task[Either[AppError, Int]] =
    marketSecondDb.fold(SecondNotFound.asLeft[Int])(p => p.asRight).pure[Task]

  val cAllowShortSell       = "AllowShortSell"
  val cAllowNVDR            = "AllowNVDR"
  val cAllowShortSellOnNVDR = "AllowShortSellOnNVDR"
  val cAllowTTF             = "AllowTTF"
  val cIsValidForTrading    = "IsValidForTrading"
  val cIsOddLot             = "IsOddLot"
  val cParValue             = "ParValue"
  val cIPOPrice             = "IPOPrice"
  val cSectorNumber         = "SectorNumber"
  val cUnderlyingSecCode    = "UnderlyingSecCode"
  val cUnderlyingSecName    = "UnderlyingSecName"
  val cMaturityDate         = "MaturityDate"
  val cContractMultiplier   = "ContractMultiplier"
  val cSettlMethod          = "SettlMethod"

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
  ): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError] {
        channel match {
          case Channel.eq => s"""
                                |INSERT INTO $keyTradableInstrument
                                |($cSecCode, $cSecName, $cSecType, $cSecDesc, $cAllowShortSell, $cAllowNVDR,
                                |$cAllowShortSellOnNVDR, $cAllowTTF, $cIsValidForTrading,
                                |$cIsOddLot, $cParValue, $cSectorNumber
                                |) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                                |ON DUPLICATE KEY UPDATE
                                |$cSecName=?, $cSecType=?, $cSecDesc=?, $cAllowShortSell=?, $cAllowNVDR=?,
                                |$cAllowShortSellOnNVDR=?, $cAllowTTF=?, $cIsValidForTrading=?,
                                |$cIsOddLot=?, $cParValue=?, $cSectorNumber=?
                                |""".stripMargin
          case Channel.fu =>
            s"""
                                |INSERT INTO $keyTradableInstrument
                                |($cSecCode, $cSecName, $cSecType, $cSecDesc,
                                |$cUnderlyingSecCode, $cUnderlyingSecName, $cMaturityDate, $cContractMultiplier, $cSettlMethod
                                |) VALUES(?,?,?,?,?,?,?,?,?)
                                |ON DUPLICATE KEY UPDATE
                                |$cSecName=?, $cSecType=?, $cSecDesc=?, $cUnderlyingSecCode=?, $cUnderlyingSecName=?,
                                |$cMaturityDate=?, $cContractMultiplier=?, $cSettlMethod=?
                                |""".stripMargin
        }
      }
      params <- EitherT.rightT[Task, AppError] {
        val secCode = oid.value
        val secName = symbol.value

        channel match {
          case Channel.eq =>
            Vector(
              secCode,
              secName,
              secType,
              secDesc,
              allowShortSell.asInstanceOf[Char].toString,
              allowNVDR.asInstanceOf[Char].toString,
              allowShortSellOnNVDR.asInstanceOf[Char].toString,
              allowTTF.asInstanceOf[Char].toString,
              isValidForTrading.asInstanceOf[Char].toString,
              isOddLot,
              parValue,
              sectorNumber,
              // Insert ends here, update startds here
              secName,
              secType,
              secDesc,
              allowShortSell.asInstanceOf[Char].toString,
              allowNVDR.asInstanceOf[Char].toString,
              allowShortSellOnNVDR.asInstanceOf[Char].toString,
              allowTTF.asInstanceOf[Char].toString,
              isValidForTrading.asInstanceOf[Char].toString,
              isOddLot,
              parValue,
              sectorNumber
            )

          case Channel.fu =>
            Vector(
              secCode,
              secName,
              secType,
              secDesc,
              underlyingSecCode,
              underlyingSecName,
              maturityDate,
              contractMultiplier,
              settlMethod,
              // Insert ends here, update starts here
              secName,
              secType,
              secDesc,
              underlyingSecCode,
              underlyingSecName,
              maturityDate,
              contractMultiplier,
              settlMethod
            )
        }
      }
      _ <- EitherT.rightT[Task, AppError](connection.sendPreparedStatement(sql, params.asJava))
    } yield ()).value

  override def getInstrument(orderbookId: OrderbookId): Task[Either[AppError, Instrument]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT $cSecName from $keyTradableInstrument WHERE $cSecCode = ${orderbookId.value}
         |""".stripMargin
      )
      data <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql))))
      res <- EitherT.rightT[Task, AppError](
        data.getRows
          .stream()
          .toArray
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => {
            val instrument = p.getString(cSecName)
            Instrument(instrument)
          })
          .headOption
          .getOrElse(Instrument("_NA_"))
      )
    } yield res).value

  override def getLastOrderbookItem(symbol: Instrument): Task[Either[AppError, Option[OrderbookItem]]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT * FROM $orderbookTable WHERE $cSourceTime=(SELECT max($cSourceTime) FROM $orderbookTable) LIMIT 1;
         |""".stripMargin
      )
      data <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql))))
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
      asks <- EitherT.rightT[Task, AppError](item.asks.filter(_.isDefined).map(_.get))
      bids <- EitherT.rightT[Task, AppError](item.bids.filter(_.isDefined).map(_.get))
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |INSERT INTO $orderbookTable
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
      _ <- EitherT.rightT[Task, AppError](connection.sendPreparedStatement(sql, params.asJava))
    } yield ()).value

  override def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT $cVolume FROM $tickerTable WHERE $cUpdateTime=(SELECT max($cUpdateTime) FROM $tickerTable) LIMIT 1;
         |""".stripMargin
      )
      data <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql))))
      res <- EitherT.rightT[Task, AppError](
        data.getRows
          .stream()
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .headOption
          .map(p => {
            Qty(p.getLong(cVolume))
          })
          .getOrElse(Qty(0L))
      )
    } yield res).value

  val cTradeTime     = "TradeTime"
  val cSendingTime   = "SendingTime"
  val cLastPrice     = "LastPrice"
  val cBidAggressor  = "BidAggressor"
  val cAskAggressor  = "AskAggressor"
  val cIsTradeReport = "IsTradeReport"
  val cMatchType     = "MatchType"
  val cTradeQuantity = "TradeQuantity"

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
  ): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](s"""
         |INSERT INTO $tickerTable
         |($cTradeTime, $cSendingTime, $cReceivingTime, $cSeqNo, $cSecCode, $cLastPrice, $cVolume, $cBidAggressor, $cAskAggressor, $cIsTradeReport, $cMatchType)
         |VALUES
         |(?,?,?,?,?,?,?,?,,?,?,?)
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError] {
        val tradeTime     = dealDateTime
        val sendingTime   = marketTs
        val receivingTime = bananaTs
        val seqNo         = seq
        val secCode       = oid.value
        val lastPrice     = p.value
        val volume        = q.value
        val (bidAggressor, askAggressor) = aggressor.asInstanceOf[Char] match {
          case 'B' => (1, 0)
          case 'A' => (0, 1)
          case _   => (0, 0)
        }
        val isTradeReport = if (dealSource == 3) 1 else 0
        val matchType     = dealSource
        Vector(
          tradeTime,
          sendingTime,
          receivingTime,
          seqNo,
          secCode,
          lastPrice,
          volume,
          bidAggressor,
          askAggressor,
          isTradeReport,
          matchType
        )
      }
      _ <- EitherT.rightT[Task, AppError](connection.sendPreparedStatement(sql, params.asJava))
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
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      _ <- EitherT(
        saveTicker(
          oid = oid,
          symbol = symbol,
          seq = seq,
          p = p,
          q = q,
          tq = Qty(-1L),
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

  override def updateProjected(
      oid: OrderbookId,
      symbol: Instrument,
      seq: Long,
      p: Price,
      q: Qty,
      ib: Qty,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](s"""
         |INSERT INTO $projectedTable ($cProjTime, $cSendingTime, $cReceivingTime, $cSeqNo, $cSecCode, $cSecName,
         |$cProjPrice, $cProjVolume, $cProjImbalance
         |) VALUES
         |(?,?,?,?,?,?,?,?,?) DUPLICATE KEY UPDATE $cSecCode = $cSecCode
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError] {
        val projTime      = marketTs.value
        val sendingTime   = marketTs.value
        val receivingTime = bananaTs.value
        val seqNo         = seq
        val secCode       = oid.value
        val secName       = symbol.value
        val projPrice     = p.value
        val projVolume    = q.value
        val projImbalance = ib.value
        Vector(projTime, sendingTime, receivingTime, seqNo, secCode, secName, projPrice, projVolume, projImbalance)
      }
      _ <- EitherT.rightT[Task, AppError](connection.sendPreparedStatement(sql, params.asJava))
    } yield ()).value

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
      tradeTs: Long,
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](s"""
         |INSERT INTO $dayTable
         |($cTradeDate, $cOpen1Price, $cOpen2Price, $cClose1Price, $cClose2Price, $cOpenPrice, $cClosePrice,
         |$cSettlementPrice, $cHighPrice, $cLowPrice, $cVolume, $cBidAggressor, $cAskAggressor)
         |VALUES
         |(?,?,?,?,?,?,?,?,?,?,?,?,?)
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError] {
        val tradeDate = tradeTs
//        val
      }
    } yield ()).value

}
