package generator

import java.lang
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.table.api.scala.BatchTableEnvironment
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

    val seedGen: Gen[Int] = if (seedOpt.isDefined) Gen.const(seedOpt.get) else Arbitrary.arbitrary[Int]

    for {
      seed <- seedGen
      seeds = Gen.listOfN(numPartitions, Arbitrary.arbitrary[Int]).apply(Parameters.default, Seed.apply(seed)).get
    } yield
      env.fromCollection(seeds)
      .rebalance() //Make a load balanced strategy scattering a number of lists equals to numPartitions, among the available task slots
      .flatMap { xs =>
        val elements: List[A] = Gen.listOfN(numElements, g).apply(Parameters.default, Seed.apply(xs)).getOrElse(Nil)
        elements
      }
      .setParallelism(numPartitions)
  }



  def generateDataSetTableGenerator[A: ClassTag : TypeInformation](numElements: Int, numPartitions: Int, g: Gen[A], seedOpt: Option[Int] = None, auto_increment: Boolean = false)
                                                             (implicit tEnv: BatchTableEnvironment, env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment): Gen[Table] = {

    for {
      dataSet <- generateDataSetGenerator(numElements, numPartitions, g, seedOpt)
    } yield
      if(auto_increment) tEnv.fromDataSet(dataSet.zipWithIndex, '_1, '_2).orderBy('_1) else tEnv.fromDataSet(dataSet)
    //If A is primitive type, it is needed to access with 'f0 to operate. If not it has its vals names. e.g: Person -> val name and val age ==> 'name and 'age
  }
}

