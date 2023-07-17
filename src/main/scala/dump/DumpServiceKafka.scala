package com.guardian
package dump

import repo.Store

import monix.eval.Task
import org.apache.kafka.clients.producer.ProducerRecord
import zio.{Clock, Schedule, ZIO, ZLayer, durationInt}
import zio.interop.monix._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream.ZStream

import scala.io.Source

case class DumpServiceKafka(filename: String, server: String, topic: String) {
  val fileIterator                                     = Source.fromFile(filename, "ISO8859_1").getLines()
  val fileInputStream: ZStream[Any, Throwable, String] = ZStream.fromIterator(fileIterator)

  val producer: ZLayer[Any, Throwable, Producer] =
    ZLayer.scoped(
      Producer.make(
        ProducerSettings(List(server))
      )
    )

  val p: ZStream[Producer, Throwable, Nothing] = fileInputStream
    .mapZIO(p =>
      for {
        els <- ZIO.succeed(p.split(" ").toVector)
        rec  =  new ProducerRecord(topic, els(0), DumpService.hexStringToByteArray(els(1)))
      } yield rec
    )
    .via(Producer.produceAll(Serde.string, Serde.byteArray))
    .drain
}

object DumpServiceKafka {

  def setup(config: Config, filename: String): DumpServiceKafka =
    DumpServiceKafka(
      filename = filename,
      server = config.kafkaConfig.server,
      topic = config.kafkaConfig.topic.get
    )
}