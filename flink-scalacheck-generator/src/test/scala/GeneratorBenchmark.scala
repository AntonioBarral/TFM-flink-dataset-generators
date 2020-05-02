import org.scalameter.api._

import scala.io.StdIn.readInt
import org.scalacheck.{Prop, Test, Gen => SCGen}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import generator.Generator

object GeneratorBenchmark extends Bench.OfflineReport {

  private val initElements = 10000
  private val incrementElements = 50000
  private val iterations = 5
  private val maxPartitions = 8
  private val rangePartitions = 4


  /*println("This program will make a small benchmark using a property over datasetGenerator.\n" +
  "You will need to input the next parameters: ")*/
  implicit private val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList


  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

      performance of "Graph with " + partitionNumber + " worker(s)" in {
        exec.independentSamples -> partitionNumber
        Warmer.Default()
        val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
        val elementsNumber = for {
          elementNumber <- elementsGen
        } yield elementNumber

        using(elementsNumber) config (
          exec.minWarmupRuns -> 2,
          exec.maxWarmupRuns -> 5,
          exec.benchRuns -> 30
        ) in {
          value => propertyToSize(value/partitionNumber, partitionNumber).check(Test.Parameters.default.withMinSuccessfulTests(1))
          // value => Generator.generateDataSetGenerator(value/partitionNumber, partitionNumber, testGen).sample.get.first(10).collect()
        }
      }
    }


  def propertyToSize(elementsPerPartition: Int, partitions: Int): Prop = {
    val testGen = SCGen.choose(1,elementsPerPartition)
    val dGen: SCGen[DataSet[Int]] = Generator.generateDataSetGenerator(elementsPerPartition, partitions, testGen)
    Prop.forAll(dGen) {
      d: DataSet[Int] => {
        d.distinct().count() <= d.count()/partitions
      }
    }
  }
}
