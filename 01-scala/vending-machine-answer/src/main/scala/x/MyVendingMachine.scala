package x

import scala.collection.mutable
import scala.collection.mutable.{Map, HashMap}


// class to represent a 'StockedItem'
class Stock(item: String, price: Int, qty: Int) {

  def getItem(): String = {
    item
  }

  def getPrice(): Int = {
    price
  }

  def getQty: Int = {
    qty
  }

  def buy(): Stock = {
    new Stock(item, price, qty - 1)
  }

  def add(newPrice: Int, addQty: Int): Stock = {
    new Stock(item, newPrice, qty + addQty)
  }
}

// simple implementation of Vending Machine
class MyVendingMachine extends VendingMachine {

  val store: mutable.Map[String, Stock] = Map()
  var myStash = 0

  override def addStockItem(item: String, price: Int, qty: Int): Int = {
    if (store.contains(item)) {
      // adding an item with a new price automatically re-prices existing stock
      val newStock = store(item).add(price, qty)
      store.put(item, newStock)
      newStock.getQty
    } else {
      store.put(item, new Stock(item, price, qty))
      return qty
    }
  }

  override def checkPrice(item: String): Int = {
    if (store.contains(item)) {
      store(item).getPrice()
    } else {
      throw new Exception("checking price on item not in stock: " + item)
    }
  }

  override def checkStock(item: String) : Int = {
    if (store.contains(item)) {
      store(item).getQty
    } else {
      0  // error and zero are OK
    }
  }


  override def balance(): Int = {
    myStash
  }

  override def deposit(amount: Int): Int = {
    myStash += amount
    myStash
  }

  override def buy(item: String): ReturnCode = {
    if (store.contains(item)) {
      val myItem = store(item)

      if (myItem.getQty < 1)
        return SoldOut

      if (myStash  < myItem.getPrice())
        return NotEnoughMoney

      // good, go ahead and buy
      store.put(item, myItem.buy)
      myStash -= myItem.getPrice()
      return Success
    } else {
      ItemNotInStock
    }
  }



}

