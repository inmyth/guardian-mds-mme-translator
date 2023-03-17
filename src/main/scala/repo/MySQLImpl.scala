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
import java.sql.Date
import java.time._

import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.FutureConverters

class MySQLImpl(channel: Channel, val connection: Connection) extends Store(channel) {
  import MySQLImpl._
  private var marketSecondDb: Option[Int] = None
  private val t1230                       = LocalTime.parse("12:30")
  private val t1630                       = LocalTime.parse("16:30")

  val (
    maxLevel,
    orderbookTable,
    tickerTable,
    dayTable,
    projectedTable,
    tradableInstrumentTable,
    indexDayTable,
    indexTickerTable
  ) = {
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
      s"mdsdb.MDS_${which}TradableInstrument",
      "mdsdb.MDS_IndexDay",
      "mdsdb.MDS_IndexTicker"
    )
  }

  private val cOrderBookMDBidPrice: Int => String = (i: Int) => s"MDBid${i}Price"
  private val cOrderBookMDBidSize: Int => String  = (i: Int) => s"MDBid${i}Size"
  private val cOrderBookMDAskPrice: Int => String = (i: Int) => s"MDAsk${i}Price"
  private val cOrderBookMDAskSize: Int => String  = (i: Int) => s"MDAsk${i}Size"
  private val cSeqNo                              = "SeqNo"
  private val cUpdateTime                         = "UpdateTime"
  private val cSourceTime                         = "SourceTime"
  private val cReceivingTime                      = "ReceivingTime"
  private val cSecName                            = "SecName"
  private val cSecCode                            = "SecCode"
  private val cVolume                             = "Volume"
  private val cValue                              = "Value"
  private val cSecType                            = "SecType"
  private val cSecDesc                            = "SecDesc"
  private val cProjTime                           = "ProjTime"
  private val cProjPrice                          = "ProjPrice"
  private val cProjVolume                         = "ProjVolume"
  private val cProjImbalance                      = "ProjImbalance"
  private val cisFinal                            = "IsFinal"
  private val cTradeDate                          = "TradeDate"
  private val cOpen1Price                         = "Open1Price"
  private val cOpen2Price                         = "Open2Price"
  private val cClose1Price                        = "Close1Price"
  private val cClose2Price                        = "Close2Price"
  private val cOpenPrice                          = "OpenPrice"
  private val cClosePrice                         = "ClosePrice"
  private val cSettlementPrice                    = "SettlementPrice"
  private val cHighPrice                          = "HighPrice"
  private val cLowPrice                           = "LowPrice"

  override def connect(createTable: Boolean): Task[Either[AppError, Unit]] =
    if (createTable) createTables() else ().asRight.pure[Task]

  override def disconnect: Task[Either[AppError, Unit]] = ().asRight.pure[Task]

  override def saveSecond(unixSecond: Int): Task[Either[AppError, Unit]] = {
    marketSecondDb = Some(unixSecond)
    ().asRight.pure[Task]
  }

  override def getSecond: Task[Either[AppError, Int]] =
    marketSecondDb.fold(SecondNotFound.asLeft[Int])(p => p.asRight).pure[Task]

  private val cAllowShortSell       = "AllowShortSell"
  private val cAllowNVDR            = "AllowNVDR"
  private val cAllowShortSellOnNVDR = "AllowShortSellOnNVDR"
  private val cAllowTTF             = "AllowTTF"
  private val cIsValidForTrading    = "IsValidForTrading"
  private val cIsOddLot             = "IsOddLot"
  private val cParValue             = "ParValue"
  private val cIPOPrice             = "IPOPrice"
  private val cSectorNumber         = "SectorNumber"
  private val cUnderlyingSecCode    = "UnderlyingSecCode"
  private val cUnderlyingSecName    = "UnderlyingSecName"
  private val cMaturityDate         = "MaturityDate"
  private val cContractMultiplier   = "ContractMultiplier"
  private val cSettlMethod          = "SettlMethod"
  private val dateFromMessageFmt    = new java.text.SimpleDateFormat("yyyyMMDD")
  private val dateToSqlFmt          = new java.text.SimpleDateFormat("yyyy-MM-dd")

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
              allowShortSell.toChar.toString,
              allowNVDR.toChar.toString,
              allowShortSellOnNVDR.toChar.toString,
              allowTTF.toChar.toString,
              isValidForTrading.toChar.toString,
              if (lotRoundSize == 1) "Y" else "N",
              parValue,
              sectorNumber,
              microToSqlDateTime(marketTs),
              // Insert ends here, update startds here
              secName,
              secType,
              secDesc,
              allowShortSell.toChar.toString,
              allowNVDR.toChar.toString,
              allowShortSellOnNVDR.toChar.toString,
              allowTTF.toChar.toString,
              isValidForTrading.toChar.toString,
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

  private val cTradeTime     = "TradeTime"
  private val cSendingTime   = "SendingTime"
  private val cLastPrice     = "LastPrice"
  private val cBidAggressor  = "BidAggressor"
  private val cAskAggressor  = "AskAggressor"
  private val cIsTradeReport = "IsTradeReport"
  private val cMatchType     = "MatchType"

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
        val (bidAggressor, askAggressor): (Byte, Byte) = aggressor.toChar match {
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
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    channel match {
      case Channel.eq =>
        updateEquityDay(
          oid = oid,
          symbol = symbol,
          o = o,
          h = h,
          l = l,
          c = c,
          turnOverQty = turnOverQty,
          marketTs = marketTs
        )
      case Channel.fu => ().asRight.pure[Task]
    }

  override def updateMySqlSettlementPrice(
      oid: OrderbookId,
      marketTs: Micro,
      settlPrice: Price
  ): Task[Either[AppError, Unit]] =
    (for {
      marketDt <- EitherT.rightT[Task, AppError](microToSqlDateTime(marketTs))
      day      <- EitherT.rightT[Task, AppError](localDateToMySqlDate(marketDt.toLocalDate))
      sql      <- EitherT.rightT[Task, AppError](s"""
           |SELECT $cTradeDate FROM $dayTable WHERE $cTradeDate
           |LIKE "${day.toString}%" AND $cSecCode = ${oid.value}
           |""".stripMargin)
      res <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql)))
      )
      id <- EitherT.rightT[Task, AppError](
        res.getRows
          .stream()
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => {
            p.getDate(cTradeDate)
          })
          .headOption
      )
      _ <- id match {
        case Some(value) =>
          for {
            s <- EitherT.rightT[Task, AppError](
              s"""
                 |UPDATE $dayTable SET $cSettlementPrice = ? WHERE $cTradeDate = ?
                 |""".stripMargin
            )
            p <- EitherT.rightT[Task, AppError](
              Vector(
                settlPrice.value,
                value
              )
            )
            _ <- EitherT.right[AppError](
              Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(s, p.asJava)))
            )
          } yield ()

        case None => EitherT.rightT[Task, AppError](())
      }
    } yield ()).value

  def getEquityDayOf(day: java.sql.Date): Task[Either[AppError, Option[EquityDayItem]]] =
    (for {
      pastSql <- EitherT.rightT[Task, AppError](s"""
         |SELECT * FROM $dayTable WHERE $cTradeDate
         |LIKE "${day.toString}%"
         |""".stripMargin)
      raw <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(pastSql))))
      past <- EitherT.rightT[Task, AppError](
        raw.getRows
          .stream()
          .toArray()
          .map(_.asInstanceOf[ArrayRowData])
          .map(p => {
            EquityDayItem(
              dateTime = p.getDate(cTradeDate),
              openPrice1 = Option(p.getInt(cOpen1Price)).map(Price(_)),
              openPrice2 = Option(p.getInt(cOpen2Price)).map(Price(_)),
              closePrice1 = Option(p.getInt(cClose1Price)).map(Price(_)),
              closePrice2 = Option(p.getInt(cClose2Price)).map(Price(_)),
              h = Option(p.getInt(cHighPrice)).map(Price(_)),
              l = Option(p.getInt(cLowPrice)).map(Price(_)),
              vol = Option(p.getLong(cVolume)).map(Qty(_)),
              settlPrice = Option(p.getInt(cSettlementPrice)).map(Price(_))
            )
          })
          .headOption
      )
    } yield past).value

  def updateEquityDay(
      oid: OrderbookId,
      symbol: Instrument,
      o: Price,
      h: Price,
      l: Price,
      c: Price,
      turnOverQty: Qty,
      marketTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      marketDt <- EitherT.rightT[Task, AppError](microToSqlDateTime(marketTs))
      today    <- EitherT.rightT[Task, AppError](localDateToMySqlDate(marketDt.toLocalDate))
      past     <- EitherT(getEquityDayOf(today))
      item <- EitherT.rightT[Task, AppError] {
        if (past.isEmpty) {
          EquityDayItem(
            dateTime = marketDt,
            openPrice1 = Some(o),
            openPrice2 = None,
            closePrice1 = None,
            closePrice2 = None
          )
        }
        else {
          val p         = past.get
          val today1230 = marketDt.`with`(t1230)
          val today1630 = marketDt.`with`(t1630)
          if (p.dateTime.isBefore(today1230)) {
            if (marketDt.isAfter(today1230) && p.closePrice1.isEmpty && p.openPrice2.isEmpty) {
              p.copy(closePrice1 = Some(c), openPrice2 = Some(o))
            }
            else {
              p
            }
          }
          else if (p.dateTime.isBefore(today1630)) {
            if (marketDt.isAfter(today1630) && p.closePrice2.isEmpty) {
              p.copy(closePrice2 = Some(c))
            }
            else {
              p
            }
          }
          else {
            p
          }
        }
      }
      sql <- EitherT.rightT[Task, AppError] {
        if (past.isEmpty) {
          val s = s"""
             |INSERT INTO $dayTable ($cTradeDate, $cSecCode, $cSecName, $cOpen1Price,
             |$cOpenPrice, $cClosePrice, $cHighPrice, $cLowPrice, $cVolume)
             |VALUES(?,?,?,?,?,?,?,?,?)
             |""".stripMargin
          val p = Vector(
            marketDt,
            oid.value,
            symbol.value,
            o.value,
            o.value,
            c.value,
            h.value,
            l.value,
            turnOverQty.value
          )
          (s, p)
        }
        else {
          val s = s"""
             |UPDATE $dayTable SET $cTradeDate = ?,
             |${item.openPrice2.map(p => s"$cOpen2Price = ${p.value},").getOrElse("")}
             |${item.closePrice1.map(p => s"$cClose1Price = ${p.value},").getOrElse("")}
             |${item.closePrice2.map(p => s"$cClose2Price = ${p.value},").getOrElse("")}
             |$cOpenPrice = ?, $cClosePrice = ?, $cHighPrice = ?, $cLowPrice = ?, $cVolume = ?
             |WHERE $cTradeDate = ?
             |""".stripMargin
          val p = Vector(
            marketDt,
            o.value,
            c.value,
            h.value,
            l.value,
            turnOverQty.value,
            item.dateTime
          )
          (s, p)
        }
      }
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql._1, sql._2.asJava)))
      )
    } yield ()).value

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
      marketTs: Micro,
      bananaTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      _ <- channel match {
        case Channel.eq =>
          for {
            _ <- EitherT(
              updateIndexDay(
                oid = oid,
                symbol = symbol,
                o = o,
                h = h,
                l = l,
                c = c,
                tradedVol = tradedVol,
                tradedValue = tradedValue,
                marketTs = marketTs
              )
            )
            _ <- EitherT(
              updateIndexTicker(
                oid = oid,
                symbol = symbol,
                o = o,
                h = h,
                l = l,
                c = c,
                tradedVol = tradedVol,
                tradedValue = tradedValue,
                marketTs = marketTs
              )
            )
          } yield ()

        case Channel.fu => EitherT.rightT[Task, AppError](())
      }
    } yield ()).value

  def updateIndexDay(
      oid: OrderbookId,
      symbol: Instrument,
      o: Price8,
      h: Price8,
      l: Price8,
      c: Price8,
      tradedVol: Qty,
      tradedValue: Price8,
      marketTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      marketDt <- EitherT.rightT[Task, AppError](microToSqlDateTime(marketTs))
      today    <- EitherT.rightT[Task, AppError](localDateToMySqlDate(marketDt.toLocalDate))
      sql      <- EitherT.rightT[Task, AppError](s"""
         |INSERT INTO $indexDayTable
         |($cTradeDate, $cSecCode, $cSecName, $cOpenPrice, $cClosePrice, $cHighPrice, $cLowPrice, $cVolume, $cValue)
         |VALUES
         |(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE $cSecCode=$cSecCode;
         |""".stripMargin)
      params <- EitherT.rightT[Task, AppError](
        Vector(
          today,
          oid.value,
          symbol.value,
          o.value,
          c.value,
          h.value,
          l.value,
          tradedVol.value,
          tradedValue.value
        )
      )
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )
    } yield ()).value

  def updateIndexTicker(
      oid: OrderbookId,
      symbol: Instrument,
      o: Price8,
      h: Price8,
      l: Price8,
      c: Price8,
      tradedVol: Qty,
      tradedValue: Price8,
      marketTs: Micro
  ): Task[Either[AppError, Unit]] =
    (for {
      marketDt <- EitherT.rightT[Task, AppError](microToSqlDateTime(marketTs))
      sql      <- EitherT.rightT[Task, AppError](s"""
           |INSERT INTO $indexTickerTable
           |($cTradeDate, $cSecCode, $cSecName, $cOpenPrice, $cClosePrice, $cHighPrice, $cLowPrice, $cVolume, $cValue)
           |VALUES
           |(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE $cSecCode=$cSecCode;
           |""".stripMargin)
      params <- EitherT.rightT[Task, AppError](
        Vector(
          marketDt,
          oid.value,
          symbol.value,
          o.value,
          c.value,
          h.value,
          l.value,
          tradedVol.value,
          tradedValue.value
        )
      )
      _ <- EitherT.right[AppError](
        Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sql, params.asJava)))
      )
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

  def createTables(): Task[Either[AppError, Unit]] =
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

      eda <- EitherT.rightT[Task, AppError](s"""
             |CREATE TABLE mdsdb.MDS_EquityDay(
             |	$cTradeDate datetime(6) NOT NULL,
             |	$cSecCode int NOT NULL,
             |	$cSecName varchar(20) NULL,
             |	$cOpen1Price int NULL,
             |	$cOpen2Price int NULL,
             |	$cClose1Price int NULL,
             |	$cClose2Price int NULL,
             |  $cOpenPrice int NULL,
             |  $cClosePrice int NULL,
             |	$cSettlementPrice int NULL,
             |	$cHighPrice int NULL,
             |  $cLowPrice int NULL,
             |	$cVolume bigint NULL,
             |	$cBidAggressor int NULL,
             |	$cAskAggressor int NULL,
             | CONSTRAINT PK_MDS_EquityDay PRIMARY KEY CLUSTERED
             |(
             |	$cTradeDate ASC,
             |	$cSecCode ASC
             |));
             |""".stripMargin)
      idd <- EitherT.rightT[Task, AppError](s"""
           |CREATE TABLE mdsdb.MDS_IndexDay(
           |  $cTradeDate date NOT NULL,
           |  $cSecCode int NOT NULL,
           |  $cSecName varchar(20) NULL,
           |  $cOpenPrice bigint NULL,
           |  $cClosePrice bigint NULL,
           |  $cHighPrice bigint NULL,
           |  $cLowPrice bigint NULL,
           |  $cVolume bigint NULL,
           |  $cValue bigint NULL,
           |CONSTRAINT PK_MDS_IndexDay PRIMARY KEY CLUSTERED
           |(
           |  $cTradeDate ASC,
           |  $cSecCode ASC
           |));
           |""".stripMargin)
      idt <- EitherT.rightT[Task, AppError](s"""
           |CREATE TABLE mdsdb.MDS_IndexTicker(
           |  $cTradeDate datetime(6) NOT NULL,
           |  $cSecCode int NOT NULL,
           |  $cSecName varchar(20) NULL,
           |  $cOpenPrice bigint NULL,
           |  $cClosePrice bigint NULL,
           |  $cHighPrice bigint NULL,
           |  $cLowPrice bigint NULL,
           |  $cVolume bigint NULL,
           |  $cValue bigint NULL,
           |CONSTRAINT PK_MDS_IndexTicker PRIMARY KEY CLUSTERED
           |(
           |  $cTradeDate ASC,
           |  $cSecCode ASC
           |));
           |""".stripMargin)
      _   <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(drop))))
      _   <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(sch))))
      _   <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(eti))))
      _   <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(dti))))
      _   <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(eob))))
      _   <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(dob))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(tck.head))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(tck.last))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(prj.head))))
      _ <-
        EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(prj.last))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(eda))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(idd))))
      _ <- EitherT.right[AppError](Task.fromFuture(FutureConverters.asScala(connection.sendPreparedStatement(idt))))
    } yield ()).value
}

object MySQLImpl {
  val zoneId: ZoneId = ZoneId.of("Asia/Bangkok")

  def localDateToMySqlDate(l: LocalDate): Date = java.sql.Date.valueOf(l)
  def microToSqlDateTime(m: Micro): LocalDateTime =
    Instant
      .ofEpochMilli(m.value / 1000)
      .plusNanos(m.value % 1000 * 1000)
      .atZone(zoneId)
      .toLocalDateTime

  def fromSqlDateToMicro(p: ArrayRowData, key: String): Micro =
    Micro(
      s"${p.getDate(key).atZone(zoneId).toInstant.toEpochMilli.toString}${(p.getDate(key).getNano % 1000000 / 1000).toString}".toLong
    )

  case class LastProjectedItem(projTime: Micro, seq: Long, secCode: Int, price: Price)

  case class EquityDayItem(
      dateTime: LocalDateTime,
      openPrice1: Option[Price],
      openPrice2: Option[Price],
      closePrice1: Option[Price],
      closePrice2: Option[Price],
      h: Option[Price] = None,
      l: Option[Price] = None,
      settlPrice: Option[Price] = None,
      vol: Option[Qty] = None,
      aggressorBid: Option[Boolean] = None,
      aggressorAsk: Option[Boolean] = None
  )
}
