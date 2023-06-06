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

  val p: ZStream[Any, Throwable, Nothing] = fileInputStream
    .mapZIO(p =>
      for {
        els <- ZIO.succeed(p.split(" ").toVector)
        res <- ZIO.fromMonixTask(process.process(els(0).toLong, DumpService.hexStringToByteArray(els(1))))
        _   <- ZIO.succeed(println(res))
      } yield ()
    )
    .drain

  def connectToStore(): Task[Either[AppError, Unit]] = process.store.connect()
}

object DumpService {

  def hexStringToByteArray(s: String): Array[Byte] = {
    val len = s.length
    val data = new Array[Byte](len / 2)
    var i = 0
    while (i < len) {
      data(i / 2) = ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16)).toByte
      i += 2
    }
    data
  }
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