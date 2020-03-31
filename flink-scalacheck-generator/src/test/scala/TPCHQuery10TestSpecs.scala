import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import TPCHQuery10TestSpecs.{Customer, Lineitem, Nation, Order}
import flink_apps.TPCHQuery10
import generator.Generator

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers


object TPCHQuery10TestSpecs {
  case class Customer(name: String, address:String, nationId: Long, acctBal: Double)
  case class Order(custId: Long, orderDate: String)
  case class Lineitem(extPrice: Double, discount: Double, returnFlag: String)
  case class Nation(nation: String)
}


class TPCHQuery10TestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest {
  override val partitions: Int = 3
  override val elements: Int = 10
  override val seed: Int = 0
  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


  def rangeDates(): List[String] = {
    val sf = new SimpleDateFormat("yyyy-m-d")
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
    val totalElements = elements*partitions
    val genCustomer: Gen[Customer] = for {
      name <- Gen.oneOf("Enrique", "Juan", "Antonio", "Ramon", "Olaya", "Carla", "Pedro", "Virginia", "Maria", "Cecilia", "Marta", "Yolanda", "Javier", "Marcos", "Mario", "Luigi")
      address <- Gen.listOfN(15, Gen.alphaChar).map(_.mkString)
      nationId <- Gen.choose(1, totalElements)
      acctBal <- Gen.chooseNum(100, 10000).map(xs => (xs*100.0).round / 100.0)
    } yield Customer(name, address, nationId, acctBal)

    Generator.generateDataSetGenerator(elements, partitions, genCustomer)
  }


  /**
   * Creates customer generator with customerId and orderDate. Order id will be generated later
   * because it will always have from 1 to elements* partitions values
   * @return
   */
  def createDatasetGeneratorOrders(): Gen[DataSet[Order]] = {
    val dateList = rangeDates() // Calculates range list before assign it to a generator to avoid serialization flink problems
    val totalElements = elements*partitions

    val genOrders: Gen[Order] = for {
      custId <- Gen.choose(1, totalElements)
      orderDate <- Gen.oneOf(dateList)
    } yield Order (custId, orderDate)

    Generator.generateDataSetGenerator(elements, partitions, genOrders)
  }

  /**
   * Creates customer generator with extended price, discount and returnFlag. LineItem id will be generated later
   * because it will always have from 1 to elements*partitions values
   * @return
   */
  def createDatasetGeneratorLineItems(): Gen[DataSet[Lineitem]] = {
    val genLineItems: Gen[Lineitem] = for {
      extPrice <- Gen.chooseNum(10000, 50000).map(xs => (xs*100.0).round / 100.0)
      discount <- Gen.chooseNum(0.01, 0.1).map(xs => (xs*100.0).round / 100.0)
      returnFlag <-Gen.oneOf("N", "A", "R")
    }yield Lineitem(extPrice, discount, returnFlag)

    Generator.generateDataSetGenerator(elements, partitions, genLineItems)
  }

  /**
   * Creates customer generator with name. NationId will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return
   */
  def createDatasetGeneratorNations(): Gen[DataSet[Nation]] = {
    val genNations: Gen[Nation] = for {
      nation <- Gen.oneOf("Spain", "France", "Italy", "UK", "Greece", "Portugal", "Rusia", "Chine", "Japan", "Germany", "Netherlands", "Denmark", "Finland", "Mexico", "Brazil", "Canada")
    }yield Nation(nation)
    Generator.generateDataSetGenerator(elements, partitions, genNations)
  }

  val customersGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers()
  val ordersGen: Gen[DataSet[Order]] = createDatasetGeneratorOrders()
  val lineItemsGen: Gen[DataSet[Lineitem]] = createDatasetGeneratorLineItems()
  val nationsGen: Gen[DataSet[Nation]] = createDatasetGeneratorNations()


  val p1: Prop = Prop.forAll (customersGen, ordersGen, lineItemsGen, nationsGen) {
    (dCustomer: DataSet[Customer], dOrders: DataSet[Order], dLineItem: DataSet[Lineitem], dNations: DataSet[Nation]) =>
      val dCustomerId = dCustomer.zipWithUniqueId.map(xs => (xs._1, xs._2.name, xs._2.address, xs._2.nationId, xs._2.acctBal))
      val dOrdersId = dOrders.zipWithUniqueId.map(xs => (xs._1, xs._2.custId, xs._2.orderDate))
      val dLineItemId = dLineItem.zipWithUniqueId.map(xs => (xs._1, xs._2.extPrice, xs._2.discount, xs._2.returnFlag))
      val dNationsId = dNations.zipWithUniqueId.map(xs => (xs._1, xs._2.nation))

      val result = TPCHQuery10.TPCHQuery10Calc(dCustomerId, dOrdersId, dLineItemId, dNationsId)
      println(dCustomerId.collect())
      println(dCustomerId.collect())



      val custIds: List[Long] = result.map(xs => xs._1).collect() toList

      val dates: List[String] = dOrdersId.filter(xs => {
        custIds.contains(xs._2)
      })
        .map(xs => xs._3)
        .collect()toList

      dates.foreach(xs => {
        xs.substring(0,4).toInt must beGreaterThan(1990)
      })
      1 must_== 1

  }

  s2"Year is always greater than 1990  $p1"
}
