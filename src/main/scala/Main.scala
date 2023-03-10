package com.guardian
import AppError.ConfigError

import cats.data.EitherT
import cats.implicits.toBifunctorOps
import com.guardian
import monix.eval.Task
import monix.execution.Scheduler
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends App {
  implicit val scheduler: Scheduler = monix.execution.Scheduler.global

  private val awaitable = (for {
    conf <- EitherT.fromEither[Task](
      ConfigSource
        .file(s"config/application.conf")
        .load[Config]
        .leftMap(e => ConfigError(s"Cannot load config: $e"))
    )
    groupId <- EitherT.rightT[Task, AppError](conf.genGroupID)
    cons    <- EitherT.rightT[Task, AppError](Consumer.setup(conf, groupId))
    _       <- EitherT(cons.connectToStore)
    _       <- EitherT.right[guardian.AppError](cons.run)
  } yield ()).value.runToFuture.map {
    case Right(_) => println("App running")

    case Left(value) =>
      println(value)
      System.exit(-2)
  }
  Await.result(awaitable, Duration.Inf)
}
