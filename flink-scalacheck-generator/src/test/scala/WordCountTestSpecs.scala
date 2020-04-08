import flink_apps.WordCount
import generator.Generator
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

import scala.collection.mutable.ListBuffer

class WordCountTestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest  {
  sequential

  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  override val partitions: Int = 3
  override val elements: Int = 5
  override val seed = 0

  private val valueDatasets = List("antonio", "enrique", "juan")
  private val numDatasets = valueDatasets.size

  private var genDatasets: ListBuffer[Gen[DataSet[String]]] = ListBuffer.empty[Gen[DataSet[String]]] //Dataset with generator and word generated


  /**
   * Creates a generator with a Gen.const of that value to generate a dataset of len elements with the same value
   *
   */
  def createGenerator(value: String): Gen[DataSet[String]] = {
    Generator.generateDataSetGenerator(elements, partitions, Gen.const(value))

  }

  /**
   * Creates a generator with a Gen.alphaStr if no value is passed by parameter
   *
   */
  def createGenerator(): Gen[DataSet[String]] = {
    Generator.generateDataSetGenerator(elements, partitions, Gen.alphaStr)

  }


  for (index <- 0 until numDatasets) {
    genDatasets :+= createGenerator(valueDatasets(index))
  }

  //For test 1 in p1
  val genDatasetAntonio: Gen[DataSet[String]] = createGenerator("antonio")
  val genDatasetEnrique: Gen[DataSet[String]] = createGenerator("enrique")
  val genDatasetJuan: Gen[DataSet[String]] = createGenerator("juan")

  //For test 2 in p2
  val genDatasetRandomStrings: Gen[DataSet[String]] = createGenerator()


  //ScalaCheck test
  "Count total elements in word count" >>
    Prop.forAll(genDatasetAntonio, genDatasetEnrique, genDatasetJuan){
    (d1: DataSet[String], d2: DataSet[String], d3: DataSet[String]) =>
      val totalCount = d1.count() + d2.count() + d3.count()
      val allDatasets = d1.union(d2.union(d3))
      WordCount.wordCountCalc(allDatasets).map{case (_,t2) => t2}.sum must_== totalCount

  }.set(minTestsOk = 50)

  "Total count - elements must be different than total count" >>
    Prop.forAll(genDatasetAntonio, genDatasetEnrique, genDatasetJuan){
    (d1: DataSet[String], d2: DataSet[String], d3: DataSet[String]) =>

      val totalCount = d1.count() + d2.count() + d3.count()
      val allDatasets = d1.union(d2.union(d3))
      WordCount.wordCountCalc(allDatasets).map{case (_,t2) => t2}.sum must_!=  totalCount - elements

  }.set(minTestsOk = 1)


  "Number of words is equal to its length" >> Prop.forAll (genDatasetRandomStrings) {
    d: DataSet[String] =>
      val dataset: DataSet[String] = d.distinct().map(xs => Seq.fill(xs.length)(xs))
        .flatMap(xs => xs)

      val wordCountTupleList = WordCount.wordCountCalc(dataset)

      wordCountTupleList.foreach({xs =>
        xs._1.length must_== xs._2
      }) must_==()

  }.set(minTestsOk = 50)
}
