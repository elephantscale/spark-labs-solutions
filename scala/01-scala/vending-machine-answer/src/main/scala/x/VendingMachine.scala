package x


// return code for buying
sealed trait ReturnCode

case object Success extends ReturnCode

case object NotEnoughMoney extends ReturnCode

case object SoldOut extends ReturnCode

case object ItemNotInStock extends ReturnCode


// vending machine implementation must implement this trait
trait VendingMachine {

  // adds to stock, sets new price, increments qty
  def addStockItem(item: String, price: Int, qty: Int): Int

  // pricing for an item.  exception if not in stock
  def checkPrice(item: String): Int

  // how many left in item.  return 0 for empty / not found
  def checkStock(item: String): Int


  // put money in
  def deposit(amount: Int): Int

  // check balance
  def balance(): Int

  // buy an item
  def buy(item: String): ReturnCode
}
