import flink_apps.FlinkApps
import generator.Generator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.scalacheck.{Gen, Prop, Properties}
import org.specs2.ScalaCheck
import org.specs2.matcher.MatchResult
import org.specs2.scalacheck.Parameters

import scala.collection.mutable.ListBuffer

class WordCountTestSpecs extends org.specs2.mutable.Specification with ScalaCheck with GeneratorTest  {

  implicit val typeInfo: TypeInformation[String] = TypeInformation.of(classOf[String])

  override val partitions: Int = 3
  override val elements: Int = 10
  override val seeds: List[Int] = List.empty[Int]

  val numDatasets = 3
  val valueDatasets = List("antonio", "enrique", "juan")
  var genDatasets: ListBuffer[(String, Gen[DataSet[String]])] = ListBuffer.empty[(String, Gen[DataSet[String]])] //Dataset with generator and word generated


  /**
   * Creates a generator with a Gen.const of that value to generate a dataset of len elements with the same value
   *
   */
   def createGenerator(value: String): Gen[DataSet[String]] = {
   Generator.generateDataSetGenerator(elements, partitions, Gen.const(value))

  }

  """override def beforeAll(): Unit = {
    for (index <- 0 until numDatasets) {
      genDatasets :+= (Gen.const(valueDatasets(index)).sample.get, createGenerator(valueDatasets(index)))
    }

  }"""

  for (index <- 0 until numDatasets) {
    genDatasets :+= (Gen.const(valueDatasets(index)).sample.get, createGenerator(valueDatasets(index)))
  }

  s2"""
 Generator generates the same number of elements wordCount counts $EqualWordCountNumber
 Total word count elements is equal to genDataset elements $equalNumberOfElements
 Subbed 10 elements to count produced by Flink wordCount program so test has to fail $hasToFailBeacuseOfSub10
"""

  def EqualWordCountNumber: MatchResult[Any] = {
    var wordGenValues: Map[String, Long] = Map()

    //First dataset is managed in a particular case
    var totalDatasets = genDatasets(0)._2.sample.get
    wordGenValues += genDatasets(0)._1 -> totalDatasets.count()

    for (index <- 1 until numDatasets) {
      val dataset = genDatasets(index)._2.sample.get
      totalDatasets = totalDatasets.union(dataset)
      wordGenValues +=  genDatasets(index)._1 -> dataset.count()
    }

    val wordCountValues = FlinkApps.wordCount(totalDatasets)

    wordCountValues.foreach({
      xs =>
        wordGenValues(xs._1) must_==(xs._2)
    })

  } must_== ()

  //ScalaCheck test
  implicit val params = Parameters(minTestsOk = 20)
  val equalNumberOfElements: Properties = new Properties("EqualNumberOfElements") {
    var totalDatasets = genDatasets(0)._2.sample.get
    val wordCountValues = FlinkApps.wordCount(totalDatasets)
    for (index <- 0 until numDatasets) {

      property("word = " + genDatasets(index)._1) = Prop.forAll(genDatasets(index)._2) { dataset =>
        dataset.count() == wordCountValues(0)._2
      }
    }
  }

  val hasToFailBeacuseOfSub10: Properties = new Properties("HasToFailBeacuseOfSub10") {
    var totalDatasets = genDatasets(0)._2.sample.get
    val wordCountValues = FlinkApps.wordCount(totalDatasets)
    for (index <- 0 until numDatasets) {

      property("word = " + genDatasets(index)._1) = Prop.forAll(genDatasets(index)._2) { dataset =>
        dataset.count() == wordCountValues(0)._2 - 10
      }
    }
  }

}
