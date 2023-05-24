package com.guardian

import Config.DbType
import repo.Store

import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.KafkaConsumerConfig
import monix.kafka.config.AutoOffsetReset
import org.apache.logging.log4j.scala.Logging
import zio.interop.monix._
import zio.kafka.consumer._
import zio.kafka.serde.Serde
import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

case class ConsumerService(consumerConfig: KafkaConsumerConfig, topic: String, process: Process, batchSize: Option[Int])
    extends Logging {
  implicit val scheduler: Scheduler = monix.execution.Scheduler.global
  private val bs: Int               = batchSize.getOrElse(10000)
  private val store                 = process.store

  val consumer: ZLayer[Any, Throwable, Consumer] =
    ZLayer.scoped(
      zio.kafka.consumer.Consumer.make(
        ConsumerSettings(consumerConfig.bootstrapServers)
          .withGroupId(topic)
      )
    )

  val c: ZStream[Consumer, Throwable, Nothing] =
    Consumer
      .plainStream(Subscription.topics(topic), Serde.byteArray, Serde.byteArray)
      .mapZIO(p =>
        for {
          seq <- ZIO.succeed(new String(p.key).toLong)
          _   <- ZIO.fromMonixTask(process.process(seq, p.value))
        } yield p
      )
      .map(_.offset)
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .drain

  def connectToStore(): Task[Either[AppError, Unit]] = store.connect()
}

object ConsumerService {

  def setup(config: Config): ConsumerService = {
    val store = config.dbType match {
      case DbType.redis => Store.redis(config.channel, config.redisConfig)
      case DbType.mysql => Store.mysql(config.channel, config.mySqlConfig)
    }
    val groupId = config.kafkaConfig.topic.getOrElse(config.genGroupID)
    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(config.kafkaConfig.server),
      groupId = groupId,
      autoOffsetReset = AutoOffsetReset.Earliest, // the starting offset when there is no offset
      maxPollInterval = 12 hours
      // you can use this settings for At Most Once semantics:
      //     observableCommitOrder = ObservableCommitOrder.BeforeAck
    )
    val process = Process(store)
    new ConsumerService(
      consumerConfig = consumerCfg,
      topic = groupId,
      process = process,
      batchSize = config.kafkaConfig.batchSize
    )
  }

}
