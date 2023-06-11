package com.guardian

import Config.{Channel, KafkaConfig, RedisConfig}

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveEnumerationReader

case class Config(
    kafkaConfig: KafkaConfig,
    redisConfig: RedisConfig,
    channel: Channel,
    filename: Option[String]
) {

  def genGroupID: String = {
    channel match {
      case Channel.eq => "set-redis"
      case Channel.fu => "tfex-redis"
    }
  }
}

object Config {
  implicit val channelConvert: ConfigReader[Channel] = deriveEnumerationReader[Channel]

  case class KafkaConfig(server: String, topic: Option[String], batchSize: Option[Int])

  case class RedisConfig(host: String, port: Int, password: Option[String])

  sealed trait Channel

  object Channel {
    case object eq extends Channel
    case object fu extends Channel
  }
}
