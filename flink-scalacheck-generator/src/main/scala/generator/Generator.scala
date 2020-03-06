package generator

import java.lang
import java.io.File
import java.io.PrintWriter
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.scalacheck.Gen
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed

import scala.reflect.ClassTag


class FaultTolerantSeeds extends RichMapPartitionFunction[Int, Int] {
  val filesPath = "/home/antonio/TFM/Code/TFM-flink-generators/flink-scalacheck-generator/src/test/resources/"
  val date = java.time.LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss"))

  override def mapPartition(values: lang.Iterable[Int], out: Collector[Int]): Unit = {
      val attempt = getRuntimeContext().getAttemptNumber
      val task = getRuntimeContext().getIndexOfThisSubtask()
      var ownElements = List.empty[Int]
      val currentFile = filesPath + date + "_worker_" + task + "_attempt_" + attempt + ".txt"
      val f = new File(currentFile)
      val print_Writer = new PrintWriter(f)

      values.forEach({
        xs =>
          ownElements :+= xs

      })
      print_Writer.write(ownElements.toString())
      print_Writer.close()
      out.collect(1)

  }
}


object Generator {

  def generateDataSetGenerator[A: ClassTag : TypeInformation](numElements: Int, numPartitions: Int, g: Gen[A])(implicit env: ExecutionEnvironment,  randomIntGen: scala.util.Random): Gen[DataSet[A]] = {

    val indexes: DataSet[(Int, Int)] = env.fromElements((0 to numPartitions -1): _*)
      .map(xs => (xs % 3, xs/*randomIntGen.nextInt()*/)) //Create a tuple dataset. First element is partition number. Second is trivial now. Could be the seed in the future


    val finalDataSet: DataSet[A] = indexes
      .partitionByHash(0) //Send each list to the partition with value equal to first position of the tuple
      .flatMap { tuple =>
        val elements: List[A] = Gen.listOfN(numElements, g).apply(Parameters.default, Seed.apply(tuple._2)).getOrElse(Nil)
        //println(tuple._1 + "--------------" + elements)
        elements
      }
      .setParallelism(3)
    finalDataSet
  }


  def main(args: Array[String]): Unit = {
    //Starts program. Set needed values
    val notArgsMessage =
      """This program needs the following arguments:
      numPartitions -> integer (compulsory)
      numElements -> integer (compulsory)
      """
    var numPartitions = 10
    var numElements = 2
    if (args.length == 0) {
      print(notArgsMessage)
      //return

    }else{
      numPartitions = args(0).toInt
      numElements = args(1).toInt
    }

    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    implicit val randomIntGenerator = scala.util.Random

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      3, // number of restart attempts
      Time.of(2, TimeUnit.SECONDS) // delay
    ))

    val genVar = Gen.choose(1, 20) // -> Gen[Int]

    val gen_dataset: Gen[DataSet[Int]] = generateDataSetGenerator(numElements, numPartitions, genVar)
    val gen_dataset_sample : DataSet[Int] = gen_dataset.sample.get

    gen_dataset_sample.mapPartition(new FaultTolerantSeeds()).count()

  }
}

