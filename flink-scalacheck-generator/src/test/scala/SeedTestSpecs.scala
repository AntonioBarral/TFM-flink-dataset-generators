import java.io.File

import scala.io.Source
import java.util.concurrent.TimeUnit

import generator.Generator
import generator.Generator.FaultTolerantSeeds
import utilities.FilesPath

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation

import org.scalacheck.Gen

import org.specs2.specification.BeforeAll



class SeedTestSpecs extends  org.specs2.mutable.Specification with BeforeAll {

  implicit val typeInfo: TypeInformation[Int] = TypeInformation.of(classOf[Int])

  private val randomIntGenerator = scala.util.Random
  private val gen = Gen.choose(0,20)
  private val elements = 3
  private val partitions = 3
  private val attempts = 3
  private val seeds = List.range(0, partitions)

  private var generatedValues : (Map[Int, List[String]], Array[Array[List[String]]])
    = (Map(), Array())


  def createGenerator(): Unit = {
    Generator.env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      2, // number of restart attempts
      Time.of(2, TimeUnit.SECONDS) // delay
    ))

    //Initialize seed list
    seeds.foreach({_ => randomIntGenerator.nextInt(partitions * 1000)})

    val genDataset = Generator.generateDataSetGenerator(elements, partitions, gen, seeds)
    genDataset.sample.get.mapPartition(new FaultTolerantSeeds[Int]).count()
  }


  def getTestGeneratedValues(tempPathMatrix :Array[Array[String]]): (Map[Int, List[String]], Array[Array[List[String]]]) = {
    var datasetValues: Map[Int, List[String]] = Map()
    val partitionValues = Array.ofDim[List[String]](partitions, attempts)

    for (attempt <- 0 to attempts-1) {
      datasetValues += (attempt -> List.empty[String])
      for (partition <- 0 to partitions-1) {

        val values  = {
          val src = Source.fromFile(tempPathMatrix(partition)(attempt))
          val line = src.getLines.next().split(", ")
          src.close
          line
        }

        datasetValues = datasetValues.updated(attempt, datasetValues.get(attempt).get ++ values)
        partitionValues(partition)(attempt) = values toList
      }
    }
    (datasetValues, partitionValues)
  }



  def beforeAll() = {
    FilesPath.initFilePathMatrix(partitions, attempts)

    try {
      createGenerator()
    } catch {
      case _: java.lang.Exception =>

    }finally {
      generatedValues = getTestGeneratedValues(FilesPath.getPathMatrix())
    }

  }

  s2"""
 Each partition has the same data in each attempt $dataByWorker
 The whole dataset is equal in each attempt $datasetByAttempt
"""

  def dataByWorker = {
    /* Map structure:
    {
       worker1: List("1234567","1234567","1234567"),
       worker2: List("98765", "98765", "98765"),
       ...
    }*/
    val partitionValues = generatedValues._2
    for (partition <- 0 to partitions-1) {
      for (attempt <- 0 to attempts - 1) {
        partitionValues(partition)(attempt) must containTheSameElementsAs(partitionValues(partition)((attempt+1) % partitionValues(partition).size))
      }
    }

  } must_== ()



  def datasetByAttempt = {
    /* Map structure:
    {
       attempt0: List(valuesPart0, valuesPart1, etc... ),
       attempt1: List(valuesPart0, valuesPart1, etc...),
       ...
    }*/
    val datasetValues = generatedValues._1
    datasetValues.foreach({
      keyVal =>
        keyVal._2 must containTheSameElementsAs (datasetValues((keyVal._1+1) % datasetValues.size) )
    })
  } must_==()
}
