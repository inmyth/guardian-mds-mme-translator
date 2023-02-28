package com.guardian

import Config.DbType
import entity._
import repo.Store

import cats.data.EitherT
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxEitherId, toTraverseOps}
import com.nasdaq.ouchitch.itch.impl.ItchMessageFactorySet
import genium.trading.itch42.messages._
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}
import monix.reactive.Observable

import java.nio.ByteBuffer

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.language.postfixOps

case class Consumer(consumerConfig: KafkaConsumerConfig, topic: String, store: Store) {
  implicit val scheduler: Scheduler = monix.execution.Scheduler.global

  private val messageFactory = new ItchMessageFactorySet()
  private val observable: Observable[Either[AppError, Unit]] =
    KafkaConsumerObservable[Array[Byte], Array[Byte]](consumerConfig, List(topic))
      .take(10000)
      .map(p => (new String(p.key()).toLong, p.value()))
      .mapEval {
        case (seq, bytes) =>
          for {
            now <- Task.now(Micro(System.nanoTime() / 1000))
            msg <- Task(messageFactory.parse(ByteBuffer.wrap(bytes)))
            res <- msg match {
              case a: SecondsMessage => store.saveSecond(a.getSeconds)

              case a: OrderBookDirectoryMessageSetImpl =>
                (for {
                  msc <- EitherT(store.getSecond)
                  mms <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  _ <- EitherT(
                    store.saveTradableInstrument(
                      OrderbookId(a.getOrderBookId),
                      Instrument(a.getSymbol),
                      secType = new String(a.getFinancialProduct), // both
                      secDesc = new String(a.getLongName),         //
                      allowShortSell = a.getAllowShortSell,
                      allowNVDR = a.getAllowNvdr,
                      allowShortSellOnNVDR = a.getAllowShortSellOnNvdr,
                      allowTTF = a.getAllowTtf,
                      isValidForTrading = a.getStatus,
                      lotRoundSize = a.getRoundLotSize,
                      parValue = a.getParValue,
                      sectorNumber = new String(a.getSectorCode),
                      underlyingSecCode = a.getUnderlying, // or underlyingOrderbookId
                      underlyingSecName = new String(a.getUnderlyingName),
                      maturityDate = a.getExpirationDate, // YYYYMMDD
                      contractMultiplier = a.getContractSize,
                      settlMethod = "NA",
                      marketTs = mms
                    )
                  )
                } yield ()).value

              case a: MarketByPriceMessage =>
                (for {
                  oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
                  symbol <- EitherT(store.getInstrument(oid))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  acts <- EitherT.rightT[Task, AppError](
                    a.getItems.asScala
                      .map(p =>
                        FlatPriceLevelAction(
                          oid = OrderbookId(a.getOrderBookId),
                          symbol = symbol,
                          marketTs = mms,
                          bananaTs = now,
                          maxLevel = a.getMaximumLevel,
                          price = Price(p.getPrice),
                          qty = Qty(p.getQuantity),
                          level = p.getLevel,
                          side = Side(p.getSide),
                          levelUpdateAction = p.getLevelUpdateAction,
                          numDeletes = p.getNumberOfDeletes
                        )
                      )
                      .toVector
                  )
                  _ <- EitherT.right[AppError](acts.map(p => store.updateOrderbook(seq, oid, p)).sequence)
                } yield ()).value

              case a: TradeTickerMessageSet =>
                (for {
                  oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
                  symbol <- EitherT(store.getInstrument(oid))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  _ <- EitherT(
                    store.updateTicker(
                      oid = oid,
                      symbol = symbol,
                      seq = seq,
                      p = Price(a.getPrice),
                      q = Qty(a.getQuantity),
                      aggressor = a.getAggressor,
                      dealSource = a.getDealSource,
                      action = a.getAction,
                      tradeReportCode = a.getTradeReportCode,
                      dealDateTime = a.getDealDateTime, //nanosec
                      marketTs = mms,
                      bananaTs = now
                    )
                  )
                } yield ()).value

              case a: EquilibriumPriceMessage =>
                (for {
                  oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
                  symbol <- EitherT(store.getInstrument(oid))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  matchedVol <- EitherT.rightT[Task, AppError](
                    Qty(if (a.getAskQuantity < a.getBidQuantity) a.getAskQuantity else a.getBidQuantity)
                  )
                  imbalanceQty <- EitherT.rightT[Task, AppError](Qty(a.getBidQuantity - a.getAskQuantity))
                  _ <- EitherT(
                    store.updateProjected(
                      oid = oid,
                      symbol = symbol,
                      seq = seq,
                      p = Price(a.getPrice),
                      q = matchedVol,
                      ib = imbalanceQty,
                      marketTs = mms,
                      bananaTs = now
                    )
                  )
                } yield ()).value

              case a: TradeStatisticsMessageSet =>
                (for {
                  symbol <- EitherT(store.getInstrument(OrderbookId(a.getOrderBookId)))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  _ <- EitherT(
                    store.updateKline(
                      symbol = symbol,
                      seq = seq,
                      o = Price(a.getOpenPrice),
                      h = Price(a.getHighPrice),
                      l = Price(a.getLowPrice),
                      c = Price(a.getLastPrice),
                      lastAuctionPx = Price(a.getLastAuctionPrice),
                      avgpx = Price(a.getAveragePrice),
                      turnOverQty = Qty(a.getTurnOverQuantity),
                      marketTs = mms,
                      bananaTs = now
                    )
                  )
                } yield ()).value

              case a: IndexPriceMessageSet =>
                (for {
                  oid    <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
                  symbol <- EitherT(store.getInstrument(oid))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  _ <- EitherT(
                    store.updateMarketStats(
                      oid = oid,
                      symbol = symbol,
                      seq = seq,
                      o = Qty(a.getOpenValue),
                      h = Qty(a.getHighValue),
                      l = Qty(a.getLowValue),
                      c = Qty(a.getValue),
                      previousClose = Qty(a.getPreviousClose),
                      tradedValue = Qty(a.getTradedValue),
                      tradedVol = Qty(a.getTradedVolume),
                      change = a.getChange,
                      changePercent = a.getChangePercent,
                      tradeTs = a.getTimestamp,
                      marketTs = mms,
                      bananaTs = now
                    )
                  )
                } yield ()).value

              case a: ReferencePriceMessage =>
                (for {
                  oid <- EitherT.rightT[Task, AppError](OrderbookId(a.getOrderBookId))
                  _ <- EitherT(
                    store.updateMySqlIPOPrice(
                      oid = oid,
                      ipoPrice = Price(a.getPrice)
                    )
                  )
                } yield ()).value

              case _ => ().asRight.pure[Task]
            }
          } yield res
      }

  private val pConsumer: monix.reactive.Consumer[Either[AppError, Unit], Unit] = monix.reactive.Consumer.complete
  def run: Task[Unit] =
    for {
      _ <- observable.consumeWith(pConsumer)
      _ <- run
    } yield ()

  def connectToStore: Task[Either[AppError, Unit]] = store.connect
}

object Consumer {

  def setup(config: Config, groupId: String): Consumer = {
    val store = config.dbType match {
      case DbType.redis => Store.redis(config.channel, config.redisConfig)
      case DbType.mysql => Store.mysql(config.channel, config.mySqlConfig)
    }
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(config.kafkaConfig.server),
      groupId = groupId,
      autoOffsetReset = AutoOffsetReset.Earliest, // the starting offset when there is no offset
      maxPollInterval = 10 minute
      // you can use this settings for At Most Once semantics:
      //     observableCommitOrder = ObservableCommitOrder.BeforeAck
    )
    new Consumer(consumerConfig = consumerCfg, topic = config.kafkaConfig.topic, store = store)
  }
}
