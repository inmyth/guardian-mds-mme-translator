package com.guardian
package repo

import Config.Channel
import entity.{Price, _}

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId}
import com.github.jasync.sql.db.Connection
import com.github.jasync.sql.db.general.ArrayRowData
import com.guardian.AppError.{MySqlError, SecondNotFound}
import monix.eval.Task

import java.time.{Instant, LocalDateTime, ZoneId}

import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.FutureConverters

class MySQLImpl(channel: Channel, connection: Connection) extends Store(channel) {
  private val zoneId: ZoneId              = ZoneId.of("Asia/Bangkok")
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

  val dateFromMessageFmt = new java.text.SimpleDateFormat("yyyyMMDD")
  val dateToSqlFmt       = new java.text.SimpleDateFormat("yyyy-MM-dd")
//  val dateTimeMicrosToSqlFmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
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
                                |INSERT INTO $tradableInstrumentTable
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
                                |INSERT INTO $tradableInstrumentTable
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
              // Insert ends here, update starts here
              secName,
              secType,
              secDesc,
              underlyingSecCode,
              underlyingSecName,
              maturityDateSql,
              contractMultiplier,
              settlMethod
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

  private def fromSqlDateToMicro(p: ArrayRowData, key: String): Micro =
    Micro(
      s"${p.getDate(key).atZone(zoneId).toInstant.toEpochMilli.toString}${(p.getDate(key).getNano % 1000000 / 1000).toString}".toLong
    )

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

  private def microToSqlDateTime(m: Micro): LocalDateTime =
    Instant
      .ofEpochMilli(m.value / 1000)
      .plusNanos(m.value % 1000 * 1000)
      .atZone(zoneId)
      .toLocalDateTime

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
      _ <- EitherT.rightT[Task, AppError](
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
           |  $cIPOPrice bigint NULL,
           |  $cSectorNumber varchar(20) NULL,
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

    } yield ()).value
}
