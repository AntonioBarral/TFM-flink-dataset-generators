import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import flink_apps.TPCHQuery10
import generator.Generator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

class TPCHQuery10TestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest{
  override val partitions: Int = 3
  override val elements: Int = 10
  override val seed: Int = 0

  case class Customer(name: String, address: String, nationId: Long, acctBal: Double)
  case class Order(custId: Long, orderDate: String)
  case class Lineitem(extPrice: Double, discount: Double, returnFlag: String)
  case class Nation(name: String)


  def rangeDates(): List[String] = {
    val sf = new SimpleDateFormat("M-d-yyyy")
    val now = new Date
    val cal = Calendar.getInstance()
    cal.set(1985, 0, 1) //Jan 1 2010

    val dates =
      Iterator.
        continually {
          val d = cal.getTime
          cal.add(Calendar.DAY_OF_YEAR, 1)
          sf.format(d)
        }.
        takeWhile(_ => cal.getTimeInMillis() <= now.getTime).
        toList

    dates
  }
  /**
   * Creates customer generator with name, address, nationId and acctbal. Customer id will be generated later
   * because it will always have from 1 to elements* partitions values
   * @return
   */
  def createDatasetGeneratorCustomers(): Gen[DataSet[Customer]] = {
    val genCustomer: Gen[Customer] = for {
      name <- Gen.listOfN(5, Gen.alphaChar).map(_.mkString) //TODO see how create possible names
      address <- Gen.listOfN(10, Gen.alphaChar).map(_.mkString) //TODO see how create possible names
      nationId <- Gen.choose(1, elements*partitions)
      acctBal <- Gen.chooseNum(500, 10000).map(xs => (xs*100.0).round / 100.0)
    } yield (Customer(name, address, nationId, acctBal))

    Generator.generateDataSetGenerator(elements, partitions, genCustomer)
  }


  /**
   * Creates customer generator with customerId and orderDate. Order id will be generated later
   * because it will always have from 1 to elements* partitions values
   * @return
   */
  def createDatasetGeneratorOrders(): Gen[DataSet[Order]] = {
    val genOrders: Gen[Order] = for {
      custId <- Gen.choose(1, elements*partitions)
      orderDate <- Gen.oneOf(rangeDates())
    } yield(Order (custId, orderDate))

    Generator.generateDataSetGenerator(elements, partitions, genOrders)
  }

  /**
   * Creates customer generator with extended price, discount and returnFlag. LineItem id will be generated later
   * because it will always have from 1 to elements* partitions values
   * @return
   */
  def createDatasetGeneratorLineItems(): Gen[DataSet[Lineitem]] = {
    val genLineItems: Gen[Lineitem] = for {
      extPrice <- Gen.chooseNum(10000, 50000).map(xs => (xs*100.0).round / 100.0)
      discount <- Gen.chooseNum(0.01, 0.1).map(xs => (xs*100.0).round / 100.0)
      returnFlag <-Gen.oneOf("N", "A", "R")
    }yield(Lineitem(extPrice, discount, returnFlag))

    Generator.generateDataSetGenerator(elements, partitions, genLineItems)
  }

  /**
   * Creates customer generator with name. NationId will be generated later
   * because it will always have from 1 to elements* partitions values
   * @return
   */
  def createDatasetGeneratorNations(): Gen[DataSet[Nation]] = {
    val genNations: Gen[Nation] = for {
      nation <- Gen.oneOf("Spain", "France", "Italy", "UK", "Greece", "Portugal", "Rusia", "Chine", "Japan", "Germany", "Netherlands", "Denmark", "Finland", "Mexico", "Brazil", "Canada")
    }yield(Nation(nation))
    Generator.generateDataSetGenerator(elements, partitions, genNations)
  }

  val customerInfo: TypeInformation[Customer] = createTypeInformation[Customer]
  val orderInfo: TypeInformation[Order] = createTypeInformation[Order]
  val lineItemInfo: TypeInformation[Lineitem] = createTypeInformation[Lineitem]
  val nationInfo: TypeInformation[Nation] = createTypeInformation[Nation]

  val customersGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers()
  val ordersGen: Gen[DataSet[Order]] = createDatasetGeneratorOrders()
  val lineItemsGen: Gen[DataSet[Lineitem]] = createDatasetGeneratorLineItems()
  val nationsGen: Gen[DataSet[Nation]] = createDatasetGeneratorNations()



}
