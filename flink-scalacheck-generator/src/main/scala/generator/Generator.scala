package generator

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.scalacheck.Gen

import scala.reflect.ClassTag


object Generator {

  def generateDataSetGenerator[A: ClassTag : TypeInformation](numElements: Int, numPartitions: Int, g: Gen[A])(implicit env: ExecutionEnvironment): Gen[DataSet[A]] = {

    val indexes: DataSet[(Int, Int)] = env.fromElements((1 to numPartitions): _*)
      .map(xs => (xs % 3, 0)) //Create a tuple dataset. First element is partition number. Second is trivial now. Could be the seed in the future

    val finalDataSet: DataSet[A] = indexes
      .partitionByRange(0) //Send each list to the partition with value equal to first position of the tuple
      .flatMap { _ =>
        val element: List[A] = Gen.listOfN(numElements, g).sample.getOrElse(Nil)
        element
      }
      .setParallelism(numPartitions)

    finalDataSet
  }


  def main(args: Array[String]): Unit = {
    //Starts program. Set needed values
    val notArgsMessage =
      """This program needs the following arguments:
      numPartitions -> integer (compulsory)
      numElements -> integer (compulsory)
      """
    var numPartitions = 3
    var numElements = 100
    if (args.length == 0) {
      print(notArgsMessage)
      //return

    }else{
      numPartitions = args(0).toInt
      numElements = args(1).toInt
    }


    implicit val env = ExecutionEnvironment.getExecutionEnvironment

    val genVar = Gen.choose(1, 20) // -> Gen[Int]

    val gen_dataset: Gen[DataSet[Int]] = generateDataSetGenerator(numElements, numPartitions, genVar)
    print(gen_dataset.sample.get.count())
  }
}

