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

import scala.jdk.CollectionConverters.CollectionHasAsScala

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
            now <- Task.now(Micro(System.currentTimeMillis() * 1000))
            msg <- Task(messageFactory.parse(ByteBuffer.wrap(bytes)))
            res <- msg match {

              case a: SecondsMessage => store.saveSecond(a.getSeconds)

              case a: OrderBookDirectoryMessageSetImpl =>
                store.saveSymbolByOrderbookId(OrderbookId(a.getOrderBookId), Instrument(a.getSymbol))

              case a: MarketByPriceMessage =>
                (for {
                  symbol <- EitherT(store.getSymbol(OrderbookId(a.getOrderBookId)))
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
                  _ <- EitherT.right[AppError](acts.map(p => store.updateOrderbook(seq, p)).sequence)
                } yield ()).value

              case a: TradeTickerMessageSet =>
                (for {
                  symbol <- EitherT(store.getSymbol(OrderbookId(a.getOrderBookId)))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  _ <- EitherT(
                    store.updateTicker(
                      symbol = symbol,
                      seq = seq,
                      p = Price(a.getPrice),
                      q = Qty(a.getQuantity),
                      aggressor = a.getAggressor,
                      dealSource = a.getDealSource,
                      action = a.getAction,
                      tradeReportCode = a.getTradeReportCode,
                      marketTs = mms,
                      bananaTs = now
                    )
                  )
                } yield ()).value

              case a: EquilibriumPriceMessage =>
                (for {
                  symbol <- EitherT(store.getSymbol(OrderbookId(a.getOrderBookId)))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  matchedVol <- EitherT.rightT[Task, AppError](
                    Qty(if (a.getAskQuantity < a.getBidQuantity) a.getAskQuantity else a.getBidQuantity)
                  )
                  imbalanceQty <- EitherT.rightT[Task, AppError](Qty(a.getBidQuantity - a.getAskQuantity))
                  _ <- EitherT(
                    store.updateProjected(
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
                  symbol <- EitherT(store.getSymbol(OrderbookId(a.getOrderBookId)))
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
                  symbol <- EitherT(store.getSymbol(OrderbookId(a.getOrderBookId)))
                  msc    <- EitherT(store.getSecond)
                  mms    <- EitherT.rightT[Task, AppError](Micro.fromSecondAndMicro(msc, a.getNanos))
                  _ <- EitherT(
                    store.updateMarketStats(
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
                      marketTs = mms,
                      bananaTs = now
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
    } yield ()
}

object Consumer {

  def setup(config: Config): Consumer = {
    val (groupId, store) = config.dbType match {
      case DbType.redis => (config.redisConfig.kafkaGroupId, Store.redis(config.channel, config.redisConfig))
      case DbType.mysql => (config.mySqlConfig.kafkaGroupId, Store.mysql(config.channel))
    }
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(config.kafkaConfig.server),
      groupId = groupId,
      autoOffsetReset = AutoOffsetReset.Earliest

      // you can use this settings for At Most Once semantics:
      //     observableCommitOrder = ObservableCommitOrder.BeforeAck
    )
    new Consumer(consumerConfig = consumerCfg, topic = config.kafkaConfig.topic, store = store)
  }
}
