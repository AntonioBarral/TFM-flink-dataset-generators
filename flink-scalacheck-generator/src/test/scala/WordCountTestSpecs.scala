import es.ucm.fdi.sscheck.matcher.specs2.flink
import flink_apps.WordCount
import generator.Generator
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

import scala.collection.mutable.ListBuffer

/**
 * Class to test DataSet Gen in WordCount example from Apache Flink github example located in [[flink_apps.WordCount]]
 */
class WordCountTestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest  {
  sequential

  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  implicit val tEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)

  override val partitions: Int = 3
  override val elements: Int = 5
  override val seed = 0

  private val valueDatasets = List("antonio", "enrique", "juan")
  private val numDatasets = valueDatasets.size

  private var genDatasets: ListBuffer[Gen[DataSet[String]]] = ListBuffer.empty[Gen[DataSet[String]]] //Dataset with generator and word generated


  /**
   * Creates a DataSet generator using a Gen.const of value passed by parameter
   *
   */
  def createGenerator(value: String): Gen[DataSet[String]] = {
    Generator.generateDataSetGenerator(elements, partitions, Gen.const(value))
  }

  /**
   * Creates a DataSet generator using a Gen.alphaStr
   *
   */
  def createGenerator(): Gen[DataSet[String]] = {
    Generator.generateDataSetGenerator(elements, partitions, Gen.alphaStr)

  }

  /**
   * Creates a Table generator using a gen of tuples (Gen.const, 1) using value passed by parameter
   *
   */
  def createTableGenerator(value: String, elements: Int): Gen[Table] = {
    val tupleGen = for {
      word <- Gen.const(value)
    }yield WordCount.WC(word, 1)

    Generator.generateDataSetTableGenerator(elements, partitions, tupleGen)
  }


  for (index <- 0 until numDatasets) {
    genDatasets :+= createGenerator(valueDatasets(index))
  }

  val genDatasetAntonio: Gen[DataSet[String]] = createGenerator("antonio")
  val genDatasetEnrique: Gen[DataSet[String]] = createGenerator("enrique")
  val genDatasetJuan: Gen[DataSet[String]] = createGenerator("juan")
  val genDatasetRandomStrings: Gen[DataSet[String]] = createGenerator()


  "Count total elements joining 3 datasets and check it is the same compared to the flink wordcount program" >>
    Prop.forAll(genDatasetAntonio, genDatasetEnrique, genDatasetJuan){
    (d1: DataSet[String], d2: DataSet[String], d3: DataSet[String]) =>
      val totalCount = d1.count() + d2.count() + d3.count()
      val allDatasets = d1.union(d2.union(d3))
      WordCount.wordCountDataSetCalc(allDatasets).map{case (_,t2) => t2}.sum must_== totalCount

  }.set(minTestsOk = 50)


  "Total count - elements must be different than total count" >>
    Prop.forAll(genDatasetAntonio, genDatasetEnrique, genDatasetJuan){
    (d1: DataSet[String], d2: DataSet[String], d3: DataSet[String]) =>

      val totalCount = d1.count() + d2.count() + d3.count()
      val allDatasets = d1.union(d2.union(d3))
      WordCount.wordCountDataSetCalc(allDatasets).map{case (_,t2) => t2}.sum must_!=  totalCount - elements

  }.set(minTestsOk = 50)


  "Number of words is equal to its length" >> Prop.forAll (genDatasetRandomStrings) {
    d: DataSet[String] =>
      val dataset: DataSet[String] = d.distinct().map(xs => Seq.fill(xs.length)(xs))
        .flatMap(xs => xs)

      val wordCountTupleList = WordCount.wordCountDataSetCalc(dataset)

      wordCountTupleList.foreach({xs =>
        xs._1.length must_== xs._2
      }) must_==()

  }.set(minTestsOk = 50)


  "Dataset generated is never empty" >> Prop.forAll(genDatasetRandomStrings) {
    d: DataSet[String] =>
      d must flink.DataSetMatchers.beNonEmptyDataSet()

  }.set(minTestsOk = 50)


  //The next test is for Table Generator, using its own WordCount example

  private val genElementst1 = Gen.choose(0,100)
  private val genElementst2 = Gen.choose(0,100)

  "Generate Table test" >> Prop.forAll(genElementst1, genElementst2) {
    (elementsT1: Int, elementsT2: Int) =>
      (elementsT1 != elementsT2) ==> {
        val t1 = createTableGenerator("foo", elementsT1).sample.get
        val t2 = createTableGenerator("bar", elementsT2).sample.get
        val tUnion = t1.unionAll(t2)
        var greater = "foo"
        if (elementsT1 < elementsT2){
          greater = "bar"
        }

        val winnerWordDataSet = WordCount.wordCountTableCalc(tUnion, Math.max(elementsT1, elementsT2)*partitions)
        winnerWordDataSet.collect().head.word == greater

      }
  }
}


