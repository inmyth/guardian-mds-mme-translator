package com.guardian

import entity.OrderbookId

sealed abstract class AppError(
    val code: Int,
    val msg: String
)

object AppError {

  case class SymbolNotFound(oid: OrderbookId) extends AppError(101, s"Symbol with orderbook id $oid not found")
  case object SecondNotFound                  extends AppError(102, "Second not found")

  case class ConfigError(m: String)          extends AppError(201, s"Cannot load config: $m")
  case class RedisConnectionError(m: String) extends AppError(202, s"Cannot connect to Redis: $m")

  case class MySqlError(m: String) extends AppError(203, s"MySQL Error: $m")

  case class OrderbookUpdateError(level: Int, currentLevel: Int)
      extends AppError(204, s"Error updating orderbook level at $level but current level: $currentLevel")
}
