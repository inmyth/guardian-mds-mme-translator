package com.guardian
package dump

import Config.DbType
import repo.Store

import monix.eval.Task
import zio.ZIO
import zio.interop.monix._
import zio.stream.ZStream

import scala.io.Source

case class DumpService(filename: String, process: Process) {
  val fileIterator                                     = Source.fromFile(filename, "ISO8859_1").getLines()
  val fileInputStream: ZStream[Any, Throwable, String] = ZStream.fromIterator(fileIterator)

  val p: ZStream[Any, Throwable, Nothing] = fileInputStream.zipWithIndex
    .mapZIO(p =>
      for {
        res <- ZIO.fromMonixTask(process.process(p._2, p._1.getBytes("ISO8859_1")))
        _   <- ZIO.succeed(println(res))
      } yield ()
    )
    .drain

  def connectToStore(): Task[Either[AppError, Unit]] = process.store.connect()
}

object DumpService {
  def setup(config: Config, filename: String): DumpService = {
    val store = config.dbType match {
      case DbType.redis => Store.redis(config.channel, config.redisConfig)
      case DbType.mysql => Store.mysql(config.channel, config.mySqlConfig)
    }
    val process = Process(store)
    new DumpService(
      filename = filename,
      process = process
    )
  }
}