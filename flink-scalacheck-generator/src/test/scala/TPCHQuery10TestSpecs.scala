import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import TPCHQuery10TestSpecs.{Customer, Lineitem, Nation, Order, toCSVFormat, validRanges, validTypes}
import es.ucm.fdi.sscheck.matcher.specs2.flink

import flink_apps.TPCHQuery10
import generator.Generator
import utilities.TableApiUDF
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers
import org.specs2.scalacheck.OneExpectationPerProp

import scala.util.control.Exception.allCatch


object TPCHQuery10TestSpecs {

  //Companion classes
  case class Customer(name: String, address:String, nationId: Long, acctBal: Double) extends Ordered [Customer] {
    override def compare(other: Customer): Int = {
      if (this.acctBal == other.acctBal)
        0
      else if (this.acctBal > other.acctBal)
        1
      else
        -1
    }
  }
  case class Order(custId: Long, orderDate: String)
  case class Lineitem(extPrice: Double, discount: Double, returnFlag: String)
  case class Nation(nation: String)


  //Companion defs
  def validRanges(c: Customer): Boolean = {
    c.acctBal <= 1000.0
  }


  def toCSVFormat(c: Customer): String = {
    c.name + ',' + c.address + ',' + c.nationId + ',' + c.acctBal
  }


  def validTypes(l: Array[String]): Boolean = {
   allCatch.opt(l(0).toInt).isEmpty && allCatch.opt(l(0).toDouble).isEmpty && allCatch.opt(l(0).toString).isDefined &&
   allCatch.opt(l(1).toInt).isEmpty && allCatch.opt(l(1).toDouble).isEmpty && allCatch.opt(l(1).toString).isDefined &&
   !l(2).contains('.') && allCatch.opt(l(2).toInt).isDefined &&
   allCatch.opt(l(3).toInt).isEmpty && allCatch.opt(l(3).toDouble).isDefined
  }
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
   * @return Generator of a dataset of customers
   */
  def createDatasetGeneratorCustomers(seed: Option[Int] = None, acctBalMin: Double = 100.0, acctBalMax: Double = 1000.0): Gen[DataSet[Customer]] = {
    val totalElements = elements * partitions

    val genCustomer: Gen[Customer] = for {
      name <- Gen.oneOf("Enrique", "Juan", "Antonio", "Ramon", "Olaya", "Carla", "Pedro", "Virginia", "Maria", "Cecilia", "Marta", "Yolanda", "Javier", "Marcos", "Mario", "Luigi")
      address <- Gen.listOfN(15, Gen.alphaChar).map(_.mkString)
      nationId <- Gen.choose(1, totalElements)
      acctBal <- Gen.chooseNum(acctBalMin, acctBalMax).map(xs => Math.round(xs * 100.0) / 100.0)
    } yield Customer(name, address, nationId, acctBal)


    Generator.generateDataSetGenerator(elements, partitions, genCustomer, seed)
  }


  /**
   * Creates customer generator with customerId and orderDate. Order id will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return Generator of a dataset of orders
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
   * @return Generator of a dataset of lineItems
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
   * @return Generator of a dataset of nations
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

  //Test2 and test3
  val seedGen1: Gen[Int] = Gen.choose(1, 100000)
  val seedGen2: Gen[Int] = Gen.choose(1, 100000)

  //ETL tests


  "Using API table over generated datasets, produces the same result when TPCHQuery is calculated with flink example function, using the same generated datasets" >>
    Prop.forAll(customersGen, ordersGen, lineItemsGen, nationsGen) {
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
          .select('c_id, 'name, 'address, 'nation, 'acctBal, ('extPrice * ('discount - 1).abs()).sum.ceil() as 'revenue) //TODO el ultimo numero puede salir con mas decimales (TPCH redondea a 4) De momento redondeo para arriba en ambos sitios

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
    }.set(minTestsOk = 20)

  "This property checks that 1 gen producing a seed to produce 2 gen datasets, generates the same gen datasets" >>
    Prop.forAll(seedGen1) {
      seed: Int =>
        val d1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))
        val d2: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))

        d1.sample.get must flink.DataSetMatchers.beSubDataSetOf(d2.sample.get)

    }.set(minTestsOk = 20)


  /**
   * The following collection of tests, simulates customer dataset generator as it was an ETL tool, with csv format and
   * with some rules to apply
   *  1. No data from header
   *  2. Number of fields is correct (4 in this test)
   *  3. Data types are the appropriate
   *  4. No numeric values with NaN or NA value
   *  5. Values in ranges defined by 'valid' function
   */

  /**
   * Checks if dataset accomplish the ETL rules defined
   *
   * @param customerStringDataset
   * @param rangeValidation
   * @param dataTypeValidation
   * @return dataset with data that accomplishes the rules
   */
  def checkCustomerETLProperties(customerStringDataset: DataSet[String], rangeValidation: Boolean = true, dataTypeValidation : Boolean = true): DataSet[Customer] = {
    customerStringDataset
      .filter {
        _ != "Name, Address, NationId, AcctBal"
      }
      .map { xs => xs.split(',') }
      .filter { xs => if(dataTypeValidation) xs.length == 4 && validTypes(xs) else xs.length == 4 }
      .map{ xs => Customer(xs(0), xs(1), xs(2).toInt, xs(3).toDouble) }
      .filter { xs => if (rangeValidation) validRanges(xs) else true }
  }


    private val validCustomerGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers()
    private val invalidCustomerGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(acctBalMin = 20000.0, acctBalMax = 30000.0)
    private val headers: DataSet[String] = env.fromElements(
      "Name, Address, NationId, AcctBal",
      "Name, Address, NationId, AcctBal",
      "Name, Address, NationId, AcctBal"
    )

    private val parseError: DataSet[String] = env.fromElements(
      "calle 1,45,234.5",
      "Fernando,Calle 2,37.6,543.1",
      "María,Calle 3,2",
      "Alfonso,Calle 4,23,45",
      "Virginia,Calle 5,NaN,678.9",
      "Carmen,Calle 6,76,NA",
    )


    "parseAndValidate: Only data belonging to validCustomer accomplishes ETL rules defined before" >>
      Prop.forAll(validCustomerGen, invalidCustomerGen) {
        (validCustomerDataset: DataSet[Customer], invalidCustomerDataset: DataSet[Customer]) =>
          val wholeDataset = headers
            .union(parseError)
            .union(validCustomerDataset.map(xs => toCSVFormat(xs)))
            .union(invalidCustomerDataset.map(xs => toCSVFormat(xs)))

          checkCustomerETLProperties(wholeDataset).collect() must containTheSameElementsAs(validCustomerDataset.collect())
      }.set(minTestsOk = 20)


  "notCheckingRanges: It has to fail because no range validation is done" >>
    Prop.forAll(validCustomerGen, invalidCustomerGen) {
      (validCustomerDataset: DataSet[Customer], invalidCustomerDataset: DataSet[Customer]) =>
        val wholeDataset = headers
          .union(parseError)
          .union(validCustomerDataset.map(xs => toCSVFormat(xs)))
          .union(invalidCustomerDataset.map(xs => toCSVFormat(xs)))

      checkCustomerETLProperties(wholeDataset, rangeValidation = false).collect() must_!=  containTheSameElementsAs(validCustomerDataset.collect())
    }.set(minTestsOk = 20)


  "notCheckingTypeCorrectness: It has to fail because no data type validation is done" >>
    Prop.forAll(validCustomerGen, invalidCustomerGen) {
      (validCustomerDataset: DataSet[Customer], invalidCustomerDataset: DataSet[Customer]) =>

        val wholeDataset = headers
          .union(parseError)
          .union(validCustomerDataset.map(xs => toCSVFormat(xs)))
          .union(invalidCustomerDataset.map(xs => toCSVFormat(xs)))

        try {
          checkCustomerETLProperties(wholeDataset, dataTypeValidation = false).collect() must_!= containTheSameElementsAs(validCustomerDataset.collect())
        } catch {
          case _: java.lang.Exception =>
            ok
        }

    }.set(minTestsOk = 100)
}
