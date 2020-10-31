package x

object QuickTest {
  def main(args: Array[String]) {
    val vending = new MyVendingMachine

    println ("Stocking 'coke', price = $1, qty = 10")
    val coke = vending.addStockItem("coke", 1, 10)
    println ("    coke stock : " + coke)
      
    println ("Stocking 'm&m', price = $2, qty = 20")
    val mm = vending.addStockItem("m&m", 2, 20)
    println ("    m&m stock : " + mm)
      
    println ("Stocking 'oreo', price = $3, qty = 20")
    val oreo = vending.addStockItem("oreo", 3, 30)
    println ("    oreo stock : " + oreo)

    println("stock qty for coke " + vending.checkStock("coke"))
    println("price for oreo " + vending.checkPrice("oreo"))

    println ("depositing $2")
    vending.deposit(2)
    println ("trying to buy oreo @ $3 : " + vending.buy("oreo"))

    println ("trying to buy coke @ $1 : " + vending.buy("coke"))
    println ("balance : " + vending.balance())

  }

}
