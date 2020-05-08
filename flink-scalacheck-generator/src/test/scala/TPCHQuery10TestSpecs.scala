import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import TPCHQuery10TestSpecs._
import es.ucm.fdi.sscheck.matcher.specs2.flink
import flink_apps.TPCHQuery10
import generator.Generator
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.scala.utils._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{BatchTableEnvironment, _}
import org.scalacheck.Prop.propBoolean
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers
import utilities.TableApiUDF

import scala.util.control.Exception.allCatch


object TPCHQuery10TestSpecs {

  //Companion classes
  /**
   * Customer object to use it in TPCH test
   * @param name customer name
   * @param address customer address
   * @param nationId customer nation id
   * @param acctBal customer acctBal
   */
  case class Customer(name: String, address:String, nationId: Long, acctBal: Double) extends Ordered [Customer] {
    override def compare(other: Customer): Int = {
      if (this.address == other.address)
        0
      else if (this.address > other.address)
        1
      else
        -1
    }
  }

  /**
   * Order object to use it in TPCH test
   * @param custId Customer id from who did the order
   * @param orderDate Order date
   */
  case class Order(custId: Long, orderDate: String)

  /**
   * Line item object to use it in TPCH test
   * @param extPrice Line item price
   * @param discount Line item discount
   * @param returnFlag Line item flag
   */
  case class Lineitem(extPrice: Double, discount: Double, returnFlag: String)

  /**
   * Nation object to use it in TPCH test
   * @param nation nation
   */
  case class Nation(nation: String)


  /**
   * Compare if Customer range values are valid
   * @param c Customer
   * @return true if valid
   */
  def validRanges(c: Customer): Boolean = {
    c.acctBal <= 1000.0 && c.acctBal > 0.0
  }

  /**
   * Converts a Customer to a csv style in String format
   * @param c Customer
   * @return String with csv format containing Customer fields separated by commas
   */
  def toCSVFormat(c: Customer): String = {
    c.name + ',' + c.address + ',' + c.nationId + ',' + c.acctBal
  }

  /**
   * Check the correctness of every Customer field
   * @param l Array of string Customer values
   * @return true if every field is valid
   */
  def validTypes(l: Array[String]): Boolean = {
   allCatch.opt(l(0).toInt).isEmpty && allCatch.opt(l(0).toDouble).isEmpty && allCatch.opt(l(0)).isDefined &&
   allCatch.opt(l(1).toInt).isEmpty && allCatch.opt(l(1).toDouble).isEmpty && allCatch.opt(l(1)).isDefined &&
   !l(2).contains('.') && allCatch.opt(l(2).toInt).isDefined &&
   allCatch.opt(l(3).toInt).isEmpty && allCatch.opt(l(3).toDouble).isDefined
  }
}

/**
 * Class to test DataSet Gen in TPCHQuery10 example from Apache Flink github example located in [[flink_apps.TPCHQuery10]]
 */
class TPCHQuery10TestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest {
  sequential

  override val partitions: Int = 3
  override val elements: Int = 50
  override val seed: Int = 0

  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  implicit val tEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)

  /**
   * Creates a range of dates from Jan 1 1985
   * @return List of dates in String format
   */
  def rangeDates(): List[String] = {
    val sf = new SimpleDateFormat("yyyy-m-d")
    val now = new Date
    val cal = Calendar.getInstance()
    cal.set(1985, 0, 1) //Jan 1 1985

    val dates =
      Iterator.
        continually {
          val d = cal.getTime
          cal.add(Calendar.DAY_OF_YEAR, 1)
          sf.format(d)
        }.
        takeWhile(_ => cal.getTimeInMillis <= now.getTime).
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
   * Creates customer generator with name, address, nationId and acctbal. Customer id will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return Generator of a Table of customers
   */
  def createTableGeneratorCustomers(seed: Option[Int] = None, acctBalMin: Double = 100.0, acctBalMax: Double = 1000.0, auto_increment: Boolean = false): Gen[Table] = {
    val totalElements = elements * partitions

    val genCustomer: Gen[Customer] = for {
      name <- Gen.oneOf("Enrique", "Juan", "Antonio", "Ramon", "Olaya", "Carla", "Pedro", "Virginia", "Maria", "Cecilia", "Marta", "Yolanda", "Javier", "Marcos", "Mario", "Luigi")
      address <- Gen.listOfN(15, Gen.alphaChar).map(_.mkString)
      nationId <- Gen.choose(1, totalElements)
      acctBal <- Gen.chooseNum(acctBalMin, acctBalMax).map(xs => Math.round(xs * 100.0) / 100.0)
    } yield Customer(name, address, nationId, acctBal)


    Generator.generateDataSetTableGenerator(elements, partitions, genCustomer, seed, auto_increment)
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
   * Creates customer generator with customerId and orderDate. Order id will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return Generator of a Table of orders
   */
  def createTableGeneratorOrders(seed: Option[Int] = None, auto_increment: Boolean = false): Gen[Table] = {
    val dateList = rangeDates() // Calculates range list before assign it to a generator to avoid serialization flink problems
    val totalElements = elements * partitions

    val genOrders: Gen[Order] = for {
      custId <- Gen.choose(1, totalElements)
      orderDate <- Gen.oneOf(dateList)
    } yield Order(custId, orderDate)

    Generator.generateDataSetTableGenerator(elements, partitions, genOrders, seed, auto_increment)
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
   * Creates customer generator with extended price, discount and returnFlag. LineItem id will be generated later
   * because it will always have from 1 to elements*partitions values
   *
   * @return Generator of a Table of lineItems
   */
  def createTableGeneratorLineItems(seed: Option[Int] = None, auto_increment: Boolean = false): Gen[Table] = {
    val genLineItems: Gen[Lineitem] = for {
      extPrice <- Gen.chooseNum(10000.0, 50000.0).map(xs => Math.round(xs * 100.0) / 100.0)
      discount <- Gen.chooseNum(0.01, 0.1).map(xs => Math.round(xs * 100.0) / 100.0)
      returnFlag <- Gen.oneOf("N", "A", "R")
    } yield Lineitem(extPrice, discount, returnFlag)

    Generator.generateDataSetTableGenerator(elements, partitions, genLineItems, seed, auto_increment)
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


  /**
   * Creates customer generator with name. NationId will be generated later
   * because it will always have from 1 to elements* partitions values
   *
   * @return Generator of a Table of nations
   */
  def createTableGeneratorNations(seed: Option[Int] = None, auto_increment: Boolean = false): Gen[Table] = {
    val genNations: Gen[Nation] = for {
      nation <- Gen.oneOf("Spain", "France", "Italy", "UK", "Greece", "Portugal", "Rusia", "Chine", "Japan", "Germany", "Netherlands", "Denmark", "Finland", "Mexico", "Brazil", "Canada")
    } yield Nation(nation)
    Generator.generateDataSetTableGenerator(elements, partitions, genNations, seed, auto_increment)
  }




  val customersGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers()
  val ordersGen: Gen[DataSet[Order]] = createDatasetGeneratorOrders()
  val lineItemsGen: Gen[DataSet[Lineitem]] = createDatasetGeneratorLineItems()
  val nationsGen: Gen[DataSet[Nation]] = createDatasetGeneratorNations()

  val customersTableGen: Gen[Table] = createTableGeneratorCustomers(auto_increment = true)
  val ordersTableGen: Gen[Table] = createTableGeneratorOrders(auto_increment = true)
  val lineItemsTableGen: Gen[Table] = createTableGeneratorLineItems(auto_increment = true)
  val nationsTableGen: Gen[Table] = createTableGeneratorNations(auto_increment = true)

  val toIntFunct = new TableApiUDF.ToInt()


  //ETL tests
  "Using API table over generated Gen[DataSet[A]], produces the same result when TPCHQuery is calculated with flink example function, using the same generated datasets" >>
    Prop.forAll(customersGen, ordersGen, lineItemsGen, nationsGen) {
      (dCustomer: DataSet[Customer], dOrders: DataSet[Order], dLineItem: DataSet[Lineitem], dNations: DataSet[Nation]) =>
        val dCustomerId = dCustomer.zipWithUniqueId.map(xs => (xs._1, xs._2.name, xs._2.address, xs._2.nationId, xs._2.acctBal))
        val dOrdersId = dOrders.zipWithUniqueId.map(xs => (xs._1, xs._2.custId, xs._2.orderDate))
        val dLineItemId = dLineItem.zipWithUniqueId.map(xs => (xs._1, xs._2.extPrice, xs._2.discount, xs._2.returnFlag))
        val dNationsId = dNations.zipWithUniqueId.map(xs => (xs._1, xs._2.nation))

        val resultTPCH: DataSet[(Long, String, String, String, Double, Double)] = TPCHQuery10.TPCHQuery10Calc(dCustomerId, dOrdersId, dLineItemId, dNationsId)

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

        val datasetApiResult: DataSet[(Long, String, String, String, Double, Double)] = tEnv.toDataSet[(Long, String, String, String, Double, Double)](tableApiResult)

        datasetApiResult must flink.DataSetMatchers.beEqualDataSetTo(resultTPCH)


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
   * @param customerStringDataset contains rows with customer attribute values
   * @param rangeValidation if true applies a range validation
   * @param dataTypeValidation if true applies a data type validation
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

  /**
   * Creates a Dataset[String] built regarding the value of each position of the list
   * @param errorList. It contains a type of wrong field in each position
   */
  def createDifferentFieldTypesCustomer(errorList: List[String]): DataSet[String] = {
    var l = List.empty[String]
    for (element <- errorList) {
      element match {
        case "NaN" =>
          l :+= "Nombre,Calle,NaN,NaN"

        case "NA" =>
          l :+= "Nombre,Calle,NA,NA"

        case "Double" =>
          l :+= "Nombre,Calle," + Arbitrary.arbitrary[Double].sample.get.toString + "0.0"

        case "Int" =>
          l :+= "Nombre,Calle," + "0" + Arbitrary.arbitrary[Int].sample.get.toString

      }
    }

    env.fromCollection(l)
  }


    private val validCustomerGen: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers()
    private val invalidCustomerGen1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(acctBalMin = 20000.0, acctBalMax = 30000.0)
    private val invalidCustomerGen2: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(acctBalMin = -1000.0, acctBalMax = 0.0)
    private val headersNumberGen: Gen[Int] = Gen.choose(0, elements)
    private val incorrectFieldNumberGen: Gen[Int] =  Gen.choose(0, elements)
    private val parseErrorGen: Gen[List[String]] = Gen.listOfN(elements, Gen.oneOf("NaN", "NA", "Double", "Int"))

  "parseAndValidate: Only data belonging to validCustomer accomplishes ETL rules defined before" >>
    Prop.forAll(validCustomerGen, invalidCustomerGen1, invalidCustomerGen2, headersNumberGen, incorrectFieldNumberGen, parseErrorGen) {
        (validCustomerDataset: DataSet[Customer], invalidCustomerDataset1: DataSet[Customer],
         invalidCustomerDataset2: DataSet[Customer], headersNumber: Int, incorrectFieldNumber: Int, parseErrorList: List[String]) =>
          (headersNumber != 0 && incorrectFieldNumber != 0) ==> {

            val stringValidCustomerDataset: DataSet[String] = validCustomerDataset.first(incorrectFieldNumber * 2).map(xs => toCSVFormat(xs))
            val moreFieldsThanNeeded = stringValidCustomerDataset.first(incorrectFieldNumber).map(xs => xs + ",extra_field")
            val lessFieldsThanNeeded = stringValidCustomerDataset.first(incorrectFieldNumber).map(xs => xs.split(",").take(3).mkString(","))

            val wholeDataset = env.fromCollection(List.fill(headersNumber)("Name, Address, NationId, AcctBal"))
              .union(moreFieldsThanNeeded)
              .union(lessFieldsThanNeeded)
              .union(invalidCustomerDataset1.map(xs => toCSVFormat(xs)))
              .union(invalidCustomerDataset2.map(xs => toCSVFormat(xs)))
              .union(createDifferentFieldTypesCustomer(parseErrorList))
              .union(validCustomerDataset.map(xs => toCSVFormat(xs)))

            checkCustomerETLProperties(wholeDataset) must flink.DataSetMatchers.beEqualDataSetTo(validCustomerDataset)
          }

      }.set(minTestsOk = 20)


  "notCheckingRanges: It has to fail because no range validation is done" >>
    Prop.forAll(validCustomerGen, invalidCustomerGen1, invalidCustomerGen2, headersNumberGen, incorrectFieldNumberGen, parseErrorGen) {
      (validCustomerDataset: DataSet[Customer], invalidCustomerDataset1: DataSet[Customer],
       invalidCustomerDataset2: DataSet[Customer], headersNumber: Int, incorrectFieldNumber: Int, parseErrorList: List[String]) =>
        (headersNumber != 0 && incorrectFieldNumber != 0) ==> {

          val stringValidCustomerDataset: DataSet[String] = validCustomerDataset.first(incorrectFieldNumber * 2).map(xs => toCSVFormat(xs))
          val moreFieldsThanNeeded = stringValidCustomerDataset.first(incorrectFieldNumber).map(xs => xs + ",extra_field")
          val lessFieldsThanNeeded = stringValidCustomerDataset.first(incorrectFieldNumber).map(xs => xs.split(",").take(3).mkString(","))

          val wholeDataset = env.fromCollection(List.fill(headersNumber)("Name, Address, NationId, AcctBal"))
            .union(moreFieldsThanNeeded)
            .union(lessFieldsThanNeeded)
            .union(invalidCustomerDataset1.map(xs => toCSVFormat(xs)))
            .union(invalidCustomerDataset2.map(xs => toCSVFormat(xs)))
            .union(createDifferentFieldTypesCustomer(parseErrorList))
            .union(validCustomerDataset.map(xs => toCSVFormat(xs)))

          checkCustomerETLProperties(wholeDataset, rangeValidation = false) must flink.DataSetMatchers.nonBeEqualDataSetTo(validCustomerDataset)
        }

    }.set(minTestsOk = 20)


  "notCheckingTypeCorrectness: It has to fail because no data type validation is done" >>
    Prop.forAll(validCustomerGen, invalidCustomerGen1, invalidCustomerGen2, headersNumberGen, incorrectFieldNumberGen, parseErrorGen) {
      (validCustomerDataset: DataSet[Customer], invalidCustomerDataset1: DataSet[Customer],
       invalidCustomerDataset2: DataSet[Customer], headersNumber: Int, incorrectFieldNumber: Int, parseErrorList: List[String]) =>
        (headersNumber != 0 && incorrectFieldNumber != 0) ==> {

          val stringValidCustomerDataset: DataSet[String] = validCustomerDataset.first(incorrectFieldNumber * 2).map(xs => toCSVFormat(xs))
          val moreFieldsThanNeeded = stringValidCustomerDataset.first(incorrectFieldNumber).map(xs => xs + ",extra_field")
          val lessFieldsThanNeeded = stringValidCustomerDataset.first(incorrectFieldNumber).map(xs => xs.split(",").take(3).mkString(","))

          val wholeDataset = env.fromCollection(List.fill(headersNumber)("Name, Address, NationId, AcctBal"))
            .union(moreFieldsThanNeeded)
            .union(lessFieldsThanNeeded)
            .union(invalidCustomerDataset1.map(xs => toCSVFormat(xs)))
            .union(invalidCustomerDataset2.map(xs => toCSVFormat(xs)))
            .union(createDifferentFieldTypesCustomer(parseErrorList))
            .union(validCustomerDataset.map(xs => toCSVFormat(xs)))

          checkCustomerETLProperties(wholeDataset, dataTypeValidation = false).collect() must throwA[Exception]
        }

    }.set(minTestsOk = 100)



  //Generic test to compare DataSets and Tables using seeds
  val seedGen1: Gen[Int] = Gen.choose(Int.MinValue, Int.MaxValue)
  val seedGen2: Gen[Int] = Gen.choose(Int.MinValue, Int.MaxValue)

  "This property checks that 2 gens with different seeds are different always" >>
    Prop.forAll(seedGen1, seedGen2) {
      (seed1: Int, seed2: Int) =>
        (seed1 != seed2) ==> {
          val d1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed1))
          val d2: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed2))
          d1.sample.get must flink.DataSetMatchers.nonBeEqualDataSetTo(d2.sample.get)
        }

    }.set(minTestsOk = 100)


  "This property checks that 1 gen producing a seed to produce 2 gen datasets, generates the same gen datasets" >>
    Prop.forAll(seedGen1) {
      seed: Int =>
        val d1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))
        val d2: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))

        d1.sample.get must flink.DataSetMatchers.beEqualDataSetTo(d2.sample.get)

    }.set(minTestsOk = 50)


  "This property checks that 1 gen producing a seed to produce a Gen[DataSet[A]] and a Gen[Table], generates the same set of data" >>
    Prop.forAll(seedGen1) {
      seed: Int =>
        val d1: Gen[DataSet[Customer]] = createDatasetGeneratorCustomers(Some(seed))
        val t2: Gen[Table] = createTableGeneratorCustomers(Some(seed))
        val d2: DataSet[Customer] = tEnv.toDataSet(t2.sample.get)

        d1.sample.get must flink.DataSetMatchers.beEqualDataSetTo(d2)

    }.set(minTestsOk = 50)
}
