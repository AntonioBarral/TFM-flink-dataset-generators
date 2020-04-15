import scala.io.Source
import java.util.concurrent.TimeUnit

import generator.Generator
import generator.Generator.FaultTolerantSeeds
import utilities.FilesPath
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalacheck.Gen
import org.specs2.matcher.MatchResult
import org.specs2.specification.BeforeAll

class SeedTestSpecs extends  org.specs2.mutable.Specification with GeneratorTest with BeforeAll {
  sequential

  implicit val typeInfo: TypeInformation[Int] = TypeInformation.of(classOf[Int])
  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


  override val elements = 10
  override val partitions = 10

  override val seed = 10
  private val gen = Gen.choose(0,20)
  private val attempts = 3

  private var generatedValues : (Map[Int, List[String]], Array[Array[List[String]]])
    = (Map(), Array())


   def createGenerator(): Unit = {
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      attempts-1, // number of restart attempts
      Time.of(2, TimeUnit.SECONDS) // delay
    ))


    val genDataset = Generator.generateDataSetGenerator(elements, partitions, gen)
    genDataset.sample.get.mapPartition(new FaultTolerantSeeds[Int]).count()
  }


  def getTestGeneratedValues(tempPathMatrix :Array[Array[String]]): (Map[Int, List[String]], Array[Array[List[String]]]) = {
    var datasetValues: Map[Int, List[String]] = Map()
    val partitionValues = Array.ofDim[List[String]](partitions, attempts)

    for (attempt <- 0 until attempts) {
      datasetValues += (attempt -> List.empty[String])
      for (partition <- 0 until partitions) {

        val values  = {
          val src = Source.fromFile(tempPathMatrix(partition)(attempt))
          val line = src.getLines.next().split(", ")
          src.close
          line
        }.toList

        datasetValues = datasetValues.updated(attempt, datasetValues(attempt) ++ values)
        partitionValues(partition)(attempt) = values
      }
    }
    (datasetValues, partitionValues)
  }



  def beforeAll(): Unit = {
    FilesPath.initFilePathMatrix(partitions, attempts)

    try {
      createGenerator()
    } catch {
      case _: java.lang.Exception =>
        println("Restart strategy failed")

    }finally {
      generatedValues = getTestGeneratedValues(FilesPath.getPathMatrix())
    }

  }

  s2"""
 Each partition has the same data in each attempt $dataByWorker
 The whole dataset is equal in each attempt $datasetByAttempt
"""

  def dataByWorker: MatchResult[Any] = {
    /* Map structure:
    {
       worker1: List("1234567","1234567","1234567"),
       worker2: List("98765", "98765", "98765"),
       ...
    }*/
    val partitionValues = generatedValues._2
    for (partition <- 0 until partitions) {
      for (attempt <- 0 until attempts) {
        partitionValues(partition)(attempt) must containTheSameElementsAs(partitionValues(partition)((attempt+1) % partitionValues(partition).length))
      }
    }
  ok
  }



  def datasetByAttempt: MatchResult[Any] = {
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
    ok
  }
}
