package generator

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import org.scalacheck.Gen
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed

import scala.reflect.ClassTag


object Generator {

  def generateDataSetGenerator[A: ClassTag : TypeInformation](numElements: Int, numPartitions: Int, g: Gen[A])(implicit env: ExecutionEnvironment,  randomIntGen: scala.util.Random): Gen[DataSet[A]] = {

    val indexes: DataSet[(Int, Int)] = env.fromElements((1 to numPartitions): _*)
      .map(xs => (xs % 3, randomIntGen.nextInt())) //Create a tuple dataset. First element is partition number. Second is trivial now. Could be the seed in the future

    indexes.print()

    val finalDataSet: DataSet[A] = indexes
      .partitionByHash(0) //Send each list to the partition with value equal to first position of the tuple
      .flatMap { tuple =>
        val element: List[A] = Gen.listOfN(numElements, g).apply(Parameters.default, Seed.apply(tuple._2)).getOrElse(Nil)
        element
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
    var numElements = 100
    if (args.length == 0) {
      print(notArgsMessage)
      //return

    }else{
      numPartitions = args(0).toInt
      numElements = args(1).toInt
    }

    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    implicit val randomIntGenerator = scala.util.Random

    val genVar = Gen.choose(1, 20) // -> Gen[Int]

    val gen_dataset: Gen[DataSet[Int]] = generateDataSetGenerator(numElements, numPartitions, genVar)
    print(gen_dataset.sample.get.print())
  }
}

