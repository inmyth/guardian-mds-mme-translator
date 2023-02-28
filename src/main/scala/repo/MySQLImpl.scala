package com.guardian
package repo

import AppError.SecondNotFound
import Config.Channel
import entity._

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.general.ArrayRowData
import monix.eval.Task

import java.lang
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.FutureConverters

class MySQLImpl(channel: Channel, val connection: Connection) extends Store(channel) {
  import MySQLImpl._
  private var marketSecondDb: Option[Int] = None

  val (maxLevel, orderbookTable, tickerTable, dayTable, projectedTable, tradableInstrumentTable) = {
    val (ml, which) = channel match {
      case Channel.eq => (10, "Equity")
      case Channel.fu => (5, "Derivative")
    }
    (
      ml,
      s"mdsdb.MDS_${which}OrderBook",
      s"mdsdb.MDS_${which}Ticker",
      s"mdsdb.MDS_${which}Day",
      s"mdsdb.MDS_${which}ProjectedPrice",
      s"mdsdb.MDS_${which}TradableInstrument"
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
  val dateFromMessageFmt    = new java.text.SimpleDateFormat("yyyyMMDD")
  val dateToSqlFmt          = new java.text.SimpleDateFormat("yyyy-MM-dd")

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
      marketTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError] {
        channel match {
          case Channel.eq =>
            s"""
              |INSERT INTO $tradableInstrumentTable
              |($cSecCode, $cSecName, $cSecType, $cSecDesc, $cAllowShortSell, $cAllowNVDR,
              |$cAllowShortSellOnNVDR, $cAllowTTF, $cIsValidForTrading,
              |$cIsOddLot, $cParValue, $cSectorNumber, $cTradeDate
              |) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
              |ON DUPLICATE KEY UPDATE
              |$cSecName=?, $cSecType=?, $cSecDesc=?, $cAllowShortSell=?, $cAllowNVDR=?,
              |$cAllowShortSellOnNVDR=?, $cAllowTTF=?, $cIsValidForTrading=?,
              |$cIsOddLot=?, $cParValue=?, $cSectorNumber=?, $cTradeDate=?
              |""".stripMargin
          case Channel.fu =>
            s"""
              |INSERT INTO $tradableInstrumentTable
              |($cSecCode, $cSecName, $cSecType, $cSecDesc,
              |$cUnderlyingSecCode, $cUnderlyingSecName, $cMaturityDate, $cContractMultiplier, $cSettlMethod, $cTradeDate
              |) VALUES(?,?,?,?,?,?,?,?,?,?)
              |ON DUPLICATE KEY UPDATE
              |$cSecName=?, $cSecType=?, $cSecDesc=?, $cUnderlyingSecCode=?, $cUnderlyingSecName=?,
              |$cMaturityDate=?, $cContractMultiplier=?, $cSettlMethod=?, $cTradeDate=?
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
              if (lotRoundSize == 1) "Y" else "N",
              parValue,
              sectorNumber,
              microToSqlDateTime(marketTs),
              // Insert ends here, update startds here
              secName,
              secType,
              secDesc,
              allowShortSell.asInstanceOf[Char].toString,
              allowNVDR.asInstanceOf[Char].toString,
              allowShortSellOnNVDR.asInstanceOf[Char].toString,
              allowTTF.asInstanceOf[Char].toString,
              isValidForTrading.asInstanceOf[Char].toString,
              if (lotRoundSize == 1) "Y" else "N",
              parValue,
              sectorNumber,
              microToSqlDateTime(marketTs)
            )

          case Channel.fu =>
            val dateMsg         = dateFromMessageFmt.parse(maturityDate.toString)
            val maturityDateSql = dateToSqlFmt.format(dateMsg)
            Vector(
              secCode,
              secName,
              secType,
              secDesc,
              underlyingSecCode,
              underlyingSecName,
              maturityDateSql,
              contractMultiplier,
              settlMethod,
              microToSqlDateTime(marketTs),
              // Insert ends here, update starts here
              secName,
              secType,
              secDesc,
              underlyingSecCode,
              underlyingSecName,
              maturityDateSql,
              contractMultiplier,
              settlMethod,
              microToSqlDateTime(marketTs)
            )
        }
      }
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )

    } yield ()).value

  override def getInstrument(orderbookId: OrderbookId): Task[Either[AppError, Instrument]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT $cSecName from $tradableInstrumentTable WHERE $cSecCode = ${orderbookId.value}
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
         |SELECT * FROM $orderbookTable WHERE $cUpdateTime=(SELECT max($cUpdateTime) FROM $orderbookTable) LIMIT 1;
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
            val bananaTs = fromSqlDateToMicro(p, cReceivingTime)
            val marketTs = fromSqlDateToMicro(p, cSourceTime)
            val asks = (1 to maxLevel)
              .map(i => {
                val maybePx  = Option(p.getInt(cOrderBookMDAskPrice(i)))
                val maybeQty = Option(p.getLong(cOrderBookMDAskSize(i)))
                (maybePx, maybeQty) match {
                  case (None, None) => None
                  case _            => Some(Price(maybePx.fold(0)(p => p)), Qty(maybeQty.fold(0L)(p => p)), Micro(0L))
                }
              })
              .filter(_.isDefined)
            val bids = (1 to maxLevel)
              .map(i => {
                val maybePx  = Option(p.getInt(cOrderBookMDBidPrice(i)))
                val maybeQty = Option(p.getLong(cOrderBookMDBidSize(i)))
                (maybePx, maybeQty) match {
                  case (None, None) => None
                  case _            => Some(Price(maybePx.fold(0)(p => p)), Qty(maybeQty.fold(0L)(p => p)), Micro(0L))
                }
              })
              .filter(_.isDefined)
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
      sql <- EitherT.rightT[Task, AppError] {
        val bCols = if (bids.nonEmpty) {
          s"""
             |,${(1 to bids.size).map(p => cOrderBookMDBidPrice(p)).mkString(",")},
             |${(1 to bids.size).map(p => cOrderBookMDBidSize(p)).mkString(",")}
             |""".stripMargin
        }
        else {
          ""
        }
        val aCols = if (asks.nonEmpty) {
          s"""
             |,${(1 to asks.size).map(p => cOrderBookMDAskPrice(p)).mkString(",")},
             |${(1 to asks.size).map(p => cOrderBookMDAskSize(p)).mkString(",")}
             |""".stripMargin
        }
        else {
          ""
        }
        val bPar = if (bids.nonEmpty) {
          s"""
             |,${(1 to 2 * bids.size).map(_ => "?").mkString(",")}
             |""".stripMargin
        }
        else {
          ""
        }
        val aPar = if (asks.nonEmpty) {
          s"""
             |,${(1 to 2 * asks.size).map(_ => "?").mkString(",")}
             |""".stripMargin
        }
        else {
          ""
        }
        s"""
         |INSERT INTO $orderbookTable
         |($cUpdateTime, $cSourceTime, $cReceivingTime, $cSeqNo, $cSecCode,$cSecName$bCols$aCols)
         |VALUES(?,?,?,?,?,?$bPar$aPar)""".stripMargin
      }
      params <- EitherT.rightT[Task, AppError] {
        val sourceTime    = microToSqlDateTime(item.marketTs).toString
        val receivingTime = microToSqlDateTime(item.bananaTs).toString
        Vector(sourceTime, sourceTime, receivingTime, item.seq, orderbookId.value, symbol.value) ++
          bids.map(_._1.value) ++ bids.map(_._2.value) ++
          asks.map(_._1.value) ++ asks.map(_._2.value)
      }
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )
    } yield ()).value

  override def getLastTickerTotalQty(symbol: Instrument): Task[Either[AppError, Qty]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT $cVolume FROM $tickerTable WHERE $cTradeTime=(SELECT max($cTradeTime) FROM $tickerTable) LIMIT 1;
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
         |(?,?,?,?,?,?,?,?,?,?,?)
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError] {
        val tradeTime     = dealDateTime
        val sendingTime   = microToSqlDateTime(marketTs)
        val receivingTime = microToSqlDateTime(bananaTs)
        val seqNo         = seq
        val secCode       = oid.value
        val lastPrice     = p.value
        val volume        = q.value
        val (bidAggressor, askAggressor): (Byte, Byte) = aggressor.asInstanceOf[Char] match {
          case 'B' => (1, 0)
          case 'A' => (0, 1)
          case _   => (0, 0)
        }
        val isTradeReport: Byte = if (dealSource == 3) 1 else 0
        val matchType           = dealSource
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
      _ <- EitherT.rightT[Task, AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )
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

  def getLastProjected: Task[Either[AppError, Option[LastProjectedItem]]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |SELECT $cProjTime, $cSeqNo, $cSecCode, $cProjPrice FROM $projectedTable WHERE
         |$cProjTime=(SELECT max($cProjTime) FROM $projectedTable) LIMIT 1;
         |""".stripMargin
      )
      data <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql))))
      res <- EitherT.rightT[Task, AppError](
        data.getRows
          .stream()
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => {
            LastProjectedItem(
              projTime = fromSqlDateToMicro(p, cProjTime),
              seq = p.getLong(cSeqNo),
              secCode = p.getInt(cSecCode),
              price = Price(p.getInt(cProjPrice))
            )
          })
          .headOption
      )
    } yield res).value

  def getLast2ndProjectedIsFinal: Task[Either[AppError, Option[lang.Boolean]]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
           |SELECT $cisFinal, $cProjTime FROM $projectedTable ORDER BY $cProjTime DESC LIMIT 2;
           |""".stripMargin
      )
      data <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql))))
      res <- EitherT.rightT[Task, AppError](
        data.getRows
          .stream()
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => p.getBoolean(cisFinal))
          .lift(1)
      )
    } yield res).value

  def updateProjectedIsFinal(procTime: Micro, seq: Long, secCode: Int, isFinal: Boolean): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](
        s"""
         |UPDATE $projectedTable SET $cisFinal = ? WHERE $cProjTime = ? AND $cSeqNo = ? AND $cSecCode = ?
         |""".stripMargin
      )
      params <- EitherT.rightT[Task, AppError](
        Vector(
          isFinal,
          microToSqlDateTime(procTime),
          seq,
          secCode
        )
      )
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
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
      last <- EitherT(getLastProjected)
      _ <- (p, last) match {
        case (now, l) if now.value == Int.MinValue && l.isDefined && l.get.price.value >= 0L =>
          EitherT(updateProjectedIsFinal(last.get.projTime, last.get.seq, last.get.secCode, isFinal = true))
        case _ => EitherT.rightT[Task, AppError](())
      }
      sql <- EitherT.rightT[Task, AppError](s"""
         |INSERT INTO $projectedTable ($cProjTime, $cSendingTime, $cReceivingTime, $cSeqNo, $cSecCode, $cSecName,
         |$cProjPrice, $cProjVolume, $cProjImbalance
         |) VALUES
         |(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE $cSecCode = $cSecCode
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError] {
        val projTime      = microToSqlDateTime(marketTs)
        val sendingTime   = microToSqlDateTime(marketTs)
        val receivingTime = microToSqlDateTime(bananaTs)
        val seqNo         = seq
        val secCode       = oid.value
        val secName       = symbol.value
        val projPrice     = p.value
        val projVolume    = q.value
        val projImbalance = ib.value
        Vector(
          projTime,
          sendingTime,
          receivingTime,
          seqNo,
          secCode,
          secName,
          projPrice,
          projVolume,
          projImbalance
        )
      }
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )
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
      oid: OrderbookId,
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
         |($cTradeDate, $cSecCode, $cSecName, $cOpen1Price, $cOpen2Price, $cClose1Price, $cClose2Price,
         |$cSettlementPrice, $cHighPrice, $cLowPrice, $cVolume, $cBidAggressor, $cAskAggressor)
         |VALUES
         |(?,?,?,?,?,?,?,?,?,?,?,?,?)
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError] {
        val tradeDate       = tradeTs
        val secCode         = oid.value
        val secName         = symbol.value
        val openPx1         = 1
        val openPx2         = 2
        val closePx1        = 1
        val closePx2        = 2
        val settlementPrice = 1
        val highPrice       = h.value
        val lowPrice        = l.value
        val volume          = tradedVol.value
        val bidAggressor    = 1
        val askAggressor    = 2
      }
    } yield ()).value

  override def updateMySqlIPOPrice(oid: OrderbookId, ipoPrice: Price): Task[Either[AppError, Unit]] =
    (for {
      sql <- EitherT.rightT[Task, AppError](s"""
         |UPDATE $tradableInstrumentTable SET $cIPOPrice = ? WHERE $cSecCode = ?
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError](
        Vector(ipoPrice.value, oid.value)
      )
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )
    } yield ()).value

  def createTables: Task[Either[AppError, Unit]] =
    (for {
      drop <- EitherT.rightT[Task, AppError](s"""
           |DROP DATABASE mdsdb;
           |""".stripMargin)
      sch  <- EitherT.rightT[Task, AppError](s"""
           |CREATE DATABASE mdsdb CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
           |""".stripMargin)
      eti  <- EitherT.rightT[Task, AppError](s"""
           |CREATE TABLE mdsdb.MDS_EquityTradableInstrument(
           |  $cSecCode int NOT NULL,
           |  $cSecName varchar(20) NULL,
           |  $cSecType varchar(20) NULL,
           |  $cSecDesc varchar(100) NULL,
           |  $cAllowShortSell varchar(20) NULL,
           |  $cAllowNVDR varchar(20) NULL,
           |  $cAllowShortSellOnNVDR varchar(20) NULL,
           |  $cAllowTTF varchar(20) NULL,
           |  $cIsValidForTrading varchar(1) NULL,
           |  $cIsOddLot varchar(1) NULL,
           |  $cParValue bigint NULL,
           |  $cIPOPrice int NULL,
           |  $cSectorNumber varchar(20) NULL,
           |  $cTradeDate datetime(6) NULL,
           |  PRIMARY KEY ($cSecCode)
           |);
           |""".stripMargin)
      dti  <- EitherT.rightT[Task, AppError](s"""
           |CREATE TABLE mdsdb.MDS_DerivativeTradableInstrument(
           |  $cSecCode int NOT NULL,
           |  $cSecName varchar (20) NULL,
           |  $cSecType varchar (20) NULL,
           |  $cSecDesc varchar (100) NULL,
           |  $cUnderlyingSecCode int NOT NULL,
           |  $cUnderlyingSecName varchar (20) NULL,
           |  $cMaturityDate datetime (6) NULL,
           |  $cContractMultiplier decimal(14, 6) NULL,
           |  $cSettlMethod varchar (20) NULL,
           |  $cTradeDate datetime (6) NULL,
           |  PRIMARY KEY ($cSecCode)
           |);
           |""".stripMargin)
      eob <- EitherT.rightT[Task, AppError](
        s"""
           |CREATE TABLE mdsdb.MDS_EquityOrderBook(
           |  $cUpdateTime datetime (6) NOT NULL,
           |  $cSourceTime datetime (6) NULL,
           |  $cReceivingTime datetime (6) NULL,
           |  $cSeqNo bigint NOT NULL,
           |  $cSecCode int NOT NULL,
           |  $cSecName varchar (20) NULL,
           |  ${(1 to 10).map(i => s"${cOrderBookMDAskPrice(i)} int NULL").mkString(",")},
           |  ${(1 to 10).map(i => s"${cOrderBookMDAskSize(i)} bigint NULL").mkString(",")},
           |  ${(1 to 10).map(i => s"${cOrderBookMDBidPrice(i)} int NULL").mkString(",")},
           |  ${(1 to 10).map(i => s"${cOrderBookMDBidSize(i)} bigint NULL").mkString(",")},
           |  CONSTRAINT PK_MDS_EquityOrderBook PRIMARY KEY CLUSTERED
           |  (
           |    $cUpdateTime ASC,
           |    $cSeqNo ASC,
           |    $cSecCode ASC
           |  ));
           |""".stripMargin
      )
      dob <- EitherT.rightT[Task, AppError](
        s"""
           |CREATE TABLE mdsdb.MDS_DerivativeOrderBook(
           |  $cUpdateTime datetime (6) NOT NULL,
           |  $cSourceTime datetime (6) NULL,
           |  $cReceivingTime datetime (6) NULL,
           |  $cSeqNo bigint NOT NULL,
           |  $cSecCode int NOT NULL,
           |  $cSecName varchar (20) NULL,
           |  ${(1 to 5).map(i => s"${cOrderBookMDAskPrice(i)} int NULL").mkString(",")},
           |  ${(1 to 5).map(i => s"${cOrderBookMDAskSize(i)} bigint NULL").mkString(",")},
           |  ${(1 to 5).map(i => s"${cOrderBookMDBidPrice(i)} int NULL").mkString(",")},
           |  ${(1 to 5).map(i => s"${cOrderBookMDBidSize(i)} bigint NULL").mkString(",")},
           |  CONSTRAINT PK_MDS_DerivativeOrderBook PRIMARY KEY CLUSTERED
           |  (
           |    $cUpdateTime ASC,
           |    $cSeqNo ASC,
           |    $cSecCode ASC
           |  ));
           |""".stripMargin
      )
      tck <- EitherT.rightT[Task, AppError] {
        Vector("Equity", "Derivative").map(p => s"""
             |CREATE TABLE mdsdb.MDS_${p}Ticker(
             |  $cTradeTime bigint NOT NULL,
             |  $cSendingTime datetime(6) NULL,
             |  $cReceivingTime datetime(6) NULL,
             |  $cSeqNo bigint NOT NULL,
             |  $cSecCode int NOT NULL,
             |  $cSecName varchar(20) NULL,
             |  $cLastPrice decimal(14, 6) NULL,
             |  $cVolume bigint NULL,
             |  $cBidAggressor bit NOT NULL,
             |  $cAskAggressor bit NOT NULL,
             |  $cIsTradeReport bit NOT NULL,
             |  $cMatchType int NULL,
             | CONSTRAINT PK_MDS_${p}Ticker PRIMARY KEY CLUSTERED
             |(
             |	$cTradeTime ASC,
             |	$cSeqNo ASC,
             |	$cSecCode ASC
             |));
             |""".stripMargin)
      }
      prj <- EitherT.rightT[Task, AppError] {
        Vector("Equity", "Derivative").map(p => s"""
             |CREATE TABLE mdsdb.MDS_${p}ProjectedPrice(
             |	$cProjTime datetime(6) NOT NULL,
             |	$cSendingTime datetime(6) NULL,
             |	$cReceivingTime datetime(6) NULL,
             |	$cSeqNo bigint NOT NULL,
             |	$cSecCode int NOT NULL,
             |	$cSecName varchar(20) NULL,
             |	$cProjPrice int NULL,
             |	$cProjVolume bigint NULL,
             |	$cProjImbalance bigint NULL,
             |	$cisFinal bit NOT NULL DEFAULT 0,
             | CONSTRAINT PK_MDS_${p}ProjectedPrice PRIMARY KEY CLUSTERED
             |(
             |	$cProjTime ASC,
             |	$cSeqNo ASC,
             |	$cSecCode ASC
             |));
             |""".stripMargin)
      }
      day <- EitherT.rightT[Task, AppError](
        Vector("Equity", "Derivative").map(p => s"""
             |CREATE TABLE mdsdb.MDS_${p}Day(
             |	$cTradeDate bigint NOT NULL,
             |	$cSecCode int NOT NULL,
             |	$cSecName varchar(20) NULL,
             |	$cOpen1Price int NOT NULL,
             |	$cOpen2Price int NOT NULL,
             |	$cClose1Price int NULL,
             |	$cClose2Price int NULL,
             |	$cSettlementPrice int NULL,
             |	$cHighPrice int NULL,
             |  $cLowPrice int NULL,
             |	$cVolume bigint NULL,
             |	$cBidAggressor int NULL,
             |	$cAskAggressor int NULL,
             | CONSTRAINT PK_MDS_${p}Day PRIMARY KEY CLUSTERED
             |(
             |	$cTradeDate ASC,
             |	$cSecCode ASC
             |));
             |""".stripMargin)
      )
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(drop))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sch))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(eti))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(dti))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(eob))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(dob))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(tck.head))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(tck.last))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(prj.head))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(prj.last))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(day.head))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(day.last))))
    } yield ()).value
}

object MySQLImpl {
  private val zoneId: ZoneId = ZoneId.of("Asia/Bangkok")

  private def microToSqlDateTime(m: Micro): LocalDateTime =
    Instant
      .ofEpochMilli(m.value / 1000)
      .plusNanos(m.value % 1000 * 1000)
      .atZone(zoneId)
      .toLocalDateTime

  private def fromSqlDateToMicro(p: ArrayRowData, key: String): Micro =
    Micro(
      s"${p.getDate(key).atZone(zoneId).toInstant.toEpochMilli.toString}${(p.getDate(key).getNano % 1000000 / 1000).toString}".toLong
    )

  case class LastProjectedItem(projTime: Micro, seq: Long, secCode: Int, price: Price)
}
