package generator

import java.lang
import java.io.{BufferedWriter, File, FileWriter}

import org.apache.flink.api.common.functions.RichMapPartitionFunction
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
      val task = getRuntimeContext().getIndexOfThisSubtask()
      var ownElements = List.empty[String]
      val currentFile = "partition_" + task + "_attempt_" + attempt + "-"
      val f = File.createTempFile(currentFile, extension)

      FilesPath.setMatrixFilePath(task, attempt, f.getAbsolutePath) //Add path to access to it later
      val writer = new BufferedWriter(new FileWriter(f))

      values.forEach({
        xs =>
          ownElements =  xs.toString :: ownElements
      })


      for (index <- 0 to ownElements.size-1) {

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

    val indexes: DataSet[Int] = env.fromElements(0 to numPartitions-1: _*)
    val seedGen: Gen[Int] = if (seedOpt.isDefined) Gen.const(seedOpt.get) else Arbitrary.arbitrary[Int]

    for {
      seed <- seedGen
    } yield
      indexes
      .rebalance() //Send each list to the partition with value equal to first position of the tuple
      .flatMap { xs =>
        val elements: List[A] = Gen.listOfN(numElements, g).apply(Parameters.default, Seed.apply(seed + xs)).getOrElse(Nil)
        //println(tuple._1 + "--------------" + elements)
        elements
      }
      .setParallelism(numPartitions)
  }

  //Easy way to test generator
  def main(args: Array[String]): Unit = {
    /*implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val a = env.fromElements("hola", "que", "tal").map(xs => Seq.fill(xs.length)(xs))
      .flatMap(xs => xs).print()*/
    print(generateDataSetGenerator(100, 3, Gen.const("antonio")).sample.get.count())
  }

}

