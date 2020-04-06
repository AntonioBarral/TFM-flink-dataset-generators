import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import TPCHQuery10TestSpecs.{Customer, Lineitem, Nation, Order}
import flink_apps.TPCHQuery10
import generator.Generator
import utilities.TableApiUDF

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers
import org.specs2.scalacheck.OneExpectationPerProp


object TPCHQuery10TestSpecs {
  case class Customer(name: String, address:String, nationId: Long, acctBal: Double)
  case class Order(custId: Long, orderDate: String)
  case class Lineitem(extPrice: Double, discount: Double, returnFlag: String)
  case class Nation(nation: String)
}


class TPCHQuery10TestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest with OneExpectationPerProp {
  sequential

  override val partitions: Int = 3
  override val elements: Int = 10
  override val seed: Int = 0
  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  private val tEnv = BatchTableEnvironment.create(env)


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
   *
   * @return
   */
  def createDatasetGeneratorCustomers(seed: Option[Int] = None): Gen[DataSet[Customer]] = {
    val totalElements = elements * partitions
    val genCustomer: Gen[Customer] = for {
      name <- Gen.oneOf("Enrique", "Juan", "Antonio", "Ramon", "Olaya", "Carla", "Pedro", "Virginia", "Maria", "Cecilia", "Marta", "Yolanda", "Javier", "Marcos", "Mario", "Luigi")
      address <- Gen.listOfN(15, Gen.alphaChar).map(_.mkString)
      nationId <- Gen.choose(1, totalElements)
      acctBal <- Gen.chooseNum(100.0, 10000.0).map(xs => Math.round(xs * 100.0) / 100.0)
    } yield Customer(name, address, nationId, acctBal)


    Generator.generateDataSetGenerator(elements, partitions, genCustomer, seed)
  }


  /**
   * Creates customer generator with customerId and orderDate. Order id will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return
   */
  def createDatasetGeneratorOrders(seed: Option[Int] = None): Gen[DataSet[Order]] = {
    val dateList = rangeDates() // Calculates range list before assign it to a generator to avoid serialization flink problems
    val totalElements = elements * partitions

    val genOrders: Gen[Order] = for {
      custId <- Gen.choose(1, totalElements)
      orderDate <- Gen.oneOf(dateList)
    } yield Order(custId, orderDate)

    Generator.generateDataSetGenerator(elements, partitions, genOrders, seed)
  }

  /**
   * Creates customer generator with extended price, discount and returnFlag. LineItem id will be generated later
   * because it will always have from 1 to elements*partitions values
   *
   * @return
   */
  def createDatasetGeneratorLineItems(seed: Option[Int] = None): Gen[DataSet[Lineitem]] = {
    val genLineItems: Gen[Lineitem] = for {
      extPrice <- Gen.chooseNum(10000.0, 50000.0).map(xs => Math.round(xs * 100.0) / 100.0)
      discount <- Gen.chooseNum(0.01, 0.1).map(xs => Math.round(xs * 100.0) / 100.0)
      returnFlag <- Gen.oneOf("N", "A", "R")
    } yield Lineitem(extPrice, discount, returnFlag)

    Generator.generateDataSetGenerator(elements, partitions, genLineItems, seed)
  }

  /**
   * Creates customer generator with name. NationId will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return
   */
  def createDatasetGeneratorNations(seed: Option[Int] = None): Gen[DataSet[Nation]] = {
    val genNations: Gen[Nation] = for {
      nation <- Gen.oneOf("Spain", "France", "Italy", "UK", "Greece", "Portugal", "Rusia", "Chine", "Japan", "Germany", "Netherlands", "Denmark", "Finland", "Mexico", "Brazil", "Canada")
    } yield Nation(nation)
    Generator.generateDataSetGenerator(elements, partitions, genNations, seed)
  }

  //Test 1
  val customersGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers()
  val ordersGen: Gen[DataSet[Order]] = createDatasetGeneratorOrders()
  val lineItemsGen: Gen[DataSet[Lineitem]] = createDatasetGeneratorLineItems()
  val nationsGen: Gen[DataSet[Nation]] = createDatasetGeneratorNations()

  val toIntFunct = new TableApiUDF.ToInt()
  //Test2
  val seedGen1: Gen[Int] = Gen.choose(1, 100000)
  val seedGen2: Gen[Int] = Gen.choose(1, 100000)


  "Using API table over generated datasets, produces the same result when TPCHQuery is calculated with flink example function, using the same generated datasets" >>
    Prop.forAll (customersGen, ordersGen, lineItemsGen, nationsGen) {
    (dCustomer: DataSet[Customer], dOrders: DataSet[Order], dLineItem: DataSet[Lineitem], dNations: DataSet[Nation]) =>
      val dCustomerId = dCustomer.zipWithUniqueId.map(xs => (xs._1, xs._2.name, xs._2.address, xs._2.nationId, xs._2.acctBal))
      val dOrdersId = dOrders.zipWithUniqueId.map(xs => (xs._1, xs._2.custId, xs._2.orderDate))
      val dLineItemId = dLineItem.zipWithUniqueId.map(xs => (xs._1, xs._2.extPrice, xs._2.discount, xs._2.returnFlag))
      val dNationsId = dNations.zipWithUniqueId.map(xs => (xs._1, xs._2.nation))

      val resultTPCH: List[(Long, String, String, String, Double, Double)] = TPCHQuery10.TPCHQuery10Calc(dCustomerId, dOrdersId, dLineItemId, dNationsId).collect() toList

      val tableCustomers: Table = tEnv.fromDataSet(dCustomerId, 'c_id, 'name, 'address, 'nationId, 'acctBal)
      val tableOrders: Table = tEnv.fromDataSet(dOrdersId, 'o_id, 'custId, 'orderDate)
      val tableLineItems: Table = tEnv.fromDataSet(dLineItemId, 'li_id, 'extPrice, 'discount, 'returnFlag)
      val tableNations: Table = tEnv.fromDataSet(dNationsId, 'n_id, 'nation)


      val tableApiResult = tableCustomers.join(tableOrders).where('c_id === 'custId)
          .join(tableLineItems).where('o_id === 'li_id)
          .join(tableNations).where('nationId === 'n_id)
          .where(toIntFunct('orderDate.substring(0, 5)) > 1990 && 'returnFlag === "R")
          .groupBy('c_id, 'name, 'acctBal, 'nation, 'address)
          .select('c_id, 'name, 'address, 'nation, 'acctBal, ('extPrice *('discount - 1).abs()).sum as 'revenue)

      val datasetApiResult: List[(Long, String, String, String, Double, Double)] = tEnv.toDataSet[(Long, String, String, String, Double, Double)](tableApiResult).collect() toList


     datasetApiResult must containTheSameElementsAs(resultTPCH)


  }.set(minTestsOk = 100)

  "This property checks that 2 gens with different seeds are different always" >>
    Prop.forAll(seedGen1, seedGen2) {
    (seed1: Int, seed2: Int) =>
      (seed1 != seed2) ==> _
      val d1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed1))
      val d2: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed2))

      d1.sample.get.collect() must_!= containTheSameElementsAs(d2.sample.get.collect())
  }.set(minTestsOk = 1)


  "This property checks that 1 gen producing a seed to produce 2 gen datasets, generates the same gen datasets" >>
    Prop.forAll(seedGen1) {
    seed: Int =>
      val d1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))
      val d2: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))

      d1.sample.get.collect() must containTheSameElementsAs(d2.sample.get.collect())
  }.set(minTestsOk = 20)

}
