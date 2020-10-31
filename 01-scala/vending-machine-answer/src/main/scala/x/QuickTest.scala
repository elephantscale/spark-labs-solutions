package x


object QuickTest {
  def main(args: Array[String]) {
    val vending = new MyVendingMachine

    vending.addStockItem("coke", 1, 10)
    vending.addStockItem("m&m", 2, 20)
    vending.addStockItem("oreo", 3, 30)

    println("stock qty for coke " + vending.checkStock("coke"))
    println("price for oreo " + vending.checkPrice("oreo"))

    println ("depositing $2")
    vending.deposit(2)
    println ("trying to buy oreo @ $3 : " + vending.buy("oreo"))

    println ("trying to buy coke @ $1 : " + vending.buy("coke"))
    println ("balance : " + vending.balance())

  }

}
