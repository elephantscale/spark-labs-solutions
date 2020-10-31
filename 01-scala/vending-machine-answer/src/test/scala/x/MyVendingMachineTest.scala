package x

import org.specs2.mutable.Specification

class MyVendingMachineTest extends Specification {


  "accept initial inventory" in {
    new MyVendingMachine().addStockItem("candy", 2, 5) mustEqual 5
  }
  "accept additional inventory" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5) mustEqual 5
    machine.addStockItem("candy", 2, 2) mustEqual 7
  }
  "check item price" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    machine.checkPrice("candy") mustEqual 2
  }
  "accept deposit" in {
    val machine = new MyVendingMachine
    machine.deposit(1) mustEqual 1
    machine.deposit(1) mustEqual 2
    machine.deposit(0) mustEqual 2
  }

  "allow to buy item" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)

    machine.deposit(2)
    machine.buy("candy")
    machine.balance mustEqual 0
    machine.checkStock("candy") mustEqual 4
  }

  "not allow to buy without enough money" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    machine.deposit(1)
    machine.buy("candy") mustEqual NotEnoughMoney
  }

  "provide correct change" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)

    machine.deposit(1)
    machine.deposit(2)
    machine.buy("candy") mustEqual Success
    machine.balance() mustEqual 1
  }

  "throw an exception when checking price on an item out of stock" in {
    def x = {
      val machine = new MyVendingMachine
      machine.checkPrice("x")
    }
    x must throwA[Exception]
  }

}
