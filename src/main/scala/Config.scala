package com.guardian

import Config.{Channel, DbType, KafkaConfig, MySqlConfig, RedisConfig}

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveEnumerationReader

case class Config(
    kafkaConfig: KafkaConfig,
    redisConfig: RedisConfig,
    mySqlConfig: MySqlConfig,
    channel: Channel,
    dbType: DbType
) {

  def genGroupID: String = {
    channel match {
      case Channel.eq => s"set-${dbType.toString}"
      case Channel.fu => s"tfex-${dbType.toString}"
    }
  }
}

object Config {
  implicit val channelConvert: ConfigReader[Channel] = deriveEnumerationReader[Channel]
  implicit val dbTypeConvert: ConfigReader[DbType]   = deriveEnumerationReader[DbType]

  case class KafkaConfig(server: String, topic: Option[String], batchSize: Option[Int])

  case class RedisConfig(host: String, port: Int, password: Option[String])

  case class MySqlConfig(
      host: String,
      port: Int,
      user: Option[String],
      password: Option[String]
  )

  sealed trait Channel

  object Channel {
    case object eq extends Channel
    case object fu extends Channel
  }

  sealed trait DbType
  object DbType {
    case object redis extends DbType
    case object mysql extends DbType
  }
}
