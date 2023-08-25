package com.guardian
import AppError.ConfigError

import cats.implicits.toBifunctorOps
import com.guardian.dump.DumpServiceKafka
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import zio.interop.monix._
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

object Main extends ZIOAppDefault with LazyLogging {

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = {
    for {
      conf <- ZIO.fromEither(
        ConfigSource
          .file(s"config/application.conf")
          .load[Config]
          .leftMap(e => ConfigError(s"Cannot load config: $e"))
      )
//      cons <- ZIO.succeed(ConsumerService.setup(conf))
//      _    <- ZIO.fromMonixTask(cons.connectToStore())
//      _    <- cons.c.runDrain.provide(cons.consumer)
      serv <- ZIO.succeed(DumpServiceKafka(conf.filename.get, conf.kafkaConfig.server, conf.kafkaConfig.topic.get))
      _    <- serv.p.runDrain
    } yield ()
  }
}
