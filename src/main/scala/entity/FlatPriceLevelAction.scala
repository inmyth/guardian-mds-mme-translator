package com.guardian
package entity

import genium.trading.itch42.messages.PriceLevelItem

case class FlatPriceLevelAction(
    oid: OrderbookId,
    symbol: Instrument,
    marketTs: Micro,
    bananaTs: Micro,
    maxLevel: Byte,
    price: Price,
    qty: Qty,
    level: Byte,
    side: Side,
    levelUpdateAction: Char,
    numDeletes: Byte
)

object FlatPriceLevelAction {

  def apply(
      oid: OrderbookId,
      symbol: Instrument,
      sourceTs: Micro,
      serverTs: Micro,
      maxLevel: Byte,
      item: PriceLevelItem
  ) =
    new FlatPriceLevelAction(
      oid = oid,
      symbol = symbol,
      marketTs = sourceTs,
      bananaTs = serverTs,
      maxLevel = maxLevel,
      price = Price(item.getPrice),
      qty = Qty(item.getQuantity),
      level = item.getLevel,
      side = Side(item.getSide),
      levelUpdateAction = item.getLevelUpdateAction.asInstanceOf[Char],
      numDeletes = item.getNumberOfDeletes
    )
}
