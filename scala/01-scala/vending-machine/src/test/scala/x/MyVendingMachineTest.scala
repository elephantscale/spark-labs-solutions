package x

import org.specs2.mutable.Specification

class MyVendingMachineTest extends Specification {


  "accept initial inventory" in {
    new MyVendingMachine().addStockItem("candy", 2, 5) mustEqual 5
  }
  "accept additional inventory" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5) mustEqual 5
    // ## TODO-1 : stock it once more and compare the result
    machine.addStockItem("candy", 2, 2) mustEqual 7
  }
  "check item price" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    // ## TODO-2 : check price : add condition & parameter
    machine.checkPrice("candy") mustEqual 2
  }
  "accept deposit" in {
    // ## TODO-3  : create a vending machine, and add some money
    val machine = new MyVendingMachine
    machine.deposit(1) mustEqual 1
    machine.deposit(1) mustEqual 2
    machine.deposit(0) mustEqual 2
  }

  "allow to buy item" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    // TODO-4a : deposit $2
    machine.deposit(2)
    // TODO-4b : buy candy
    machine.buy("candy")
    // TODO-4c : check balance, should be zero
    machine.balance mustEqual 0
    // TODO-4d : check stock of candy, must be ???
    machine.checkStock("candy") mustEqual 4
  }

  "not allow to buy without enough money" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    machine.deposit(1)
    // TODO-5 : check for 'NotEnoughMoney' return code
    machine.buy("candy") mustEqual NotEnoughMoney
  }

  "provide correct change" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    // TODO-6 : implement this test
    //          deposit $1 and then $2,  buy candy
    //          check for 'Success' code
    //          balance must be $1
    machine.deposit(1)
    machine.deposit(2)
    machine.buy("candy") mustEqual Success
    machine.balance() mustEqual 1
  }
    
  // TODO-7 : inspect exception handling test
  "throw an exception when checking price on an item out of stock" in {
    def x = {
      val machine = new MyVendingMachine
      machine.checkPrice("x")
    }
    x must throwA[Exception]
  }
  
  // TODO-8 : bonus lab : come up with another test case
  "Check Item not in stock" in {
    val machine = new MyVendingMachine
    machine.addStockItem("candy", 2, 5)
    machine.deposit(2)
    machine.buy("oreo") mustEqual ItemNotInStock
    machine.balance() mustEqual 2
  }
}
