package com.guardian
package dump

import AppError.ConfigError

import cats.implicits.{catsSyntaxEitherId, toBifunctorOps}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import zio.interop.monix._
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

object DumpMain extends ZIOAppDefault {

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = {
    for {
      conf <- ZIO.fromEither(
        ConfigSource
          .file(s"config/application.conf")
          .load[Config]
          .leftMap(e => ConfigError(s"Cannot load config: $e"))
      )
      filename <- ZIO.fromEither(conf.filename.fold(ConfigError(s"No file dump").asLeft[String])(p => p.asRight))
      dumper   <- ZIO.succeed(DumpService.setup(conf, filename))
      _        <- ZIO.fromMonixTask(dumper.connectToStore())
      _        <- dumper.p.runDrain
    } yield ()
  }
}
