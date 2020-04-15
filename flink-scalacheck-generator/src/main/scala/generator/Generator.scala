package generator

import java.lang
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.flink.api.common.functions.{RichMapPartitionFunction, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed
import utilities.FilesPath

import scala.reflect.ClassTag


object Generator {

  private val extension = ".txt"

  class FaultTolerantSeeds[A] extends RichMapPartitionFunction[A, Int] {

    override def mapPartition(values: lang.Iterable[A], out: Collector[Int]): Unit = {
      val attempt = getRuntimeContext.getAttemptNumber
      val task = getRuntimeContext.getIndexOfThisSubtask
      var ownElements = List.empty[String]
      val currentFile = "partition_" + task + "_attempt_" + attempt + "-"
      val f = File.createTempFile(currentFile, extension)

      FilesPath.setMatrixFilePath(task, attempt, f.getAbsolutePath) //Add path to access to it later
      val writer = new BufferedWriter(new FileWriter(f))

      values.forEach({
        xs =>
          ownElements =  xs.toString :: ownElements
      })


      for (index <- ownElements.indices) {

        if (index != ownElements.size-1) {
          writer.write(ownElements(index) + ", ")
        }else{
          writer.write(ownElements(index))
        }
      }
      writer.flush()
      writer.close()

      Thread.sleep(1000)

      throw new Exception("Testing fault tolerance")

    }
  }


  def generateDataSetGenerator[A: ClassTag : TypeInformation](numElements: Int, numPartitions: Int, g: Gen[A], seedOpt: Option[Int] = None)
                                                             (implicit env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment): Gen[DataSet[A]] = {

    //val indexes: DataSet[Int] = env.fromElements(0 to numPartitions-1: _*)
    val seedGen: Gen[Int] = if (seedOpt.isDefined) Gen.const(seedOpt.get) else Arbitrary.arbitrary[Int]

    for {
      seeds <- Gen.listOfN(numPartitions, seedGen)
    } yield
      env.fromCollection(seeds)
      .rebalance() //Make a load balanced strategy scattering a number of lists equals to numPartitions, among the available task slots
      .flatMap { xs =>
        val elements: List[A] = Gen.listOfN(numElements, g).apply(Parameters.default, Seed.apply(xs)).getOrElse(Nil)
        elements
      }
      .setParallelism(numPartitions)
  }

  //Easy way to test generator
  def main(args: Array[String]): Unit = {
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val a = env.fromElements(("hola",1), ("que",2), ("tal",3))
    val b =  env.fromElements(("hola",1), ("que",2), ("tal",3))
    //print(generateDataSetGenerator(1000000, 10, Gen.choose(1,100)).sample.get.count())
  }

}

