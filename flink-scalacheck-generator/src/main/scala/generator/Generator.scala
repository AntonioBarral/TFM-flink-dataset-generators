package generator

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.KeyedStream
import org.scalacheck.Gen


object Generator {

  def generateDataSetGenerator(numElements: Int, numPartitions: Int, g: Gen[Int]) (implicit env: ExecutionEnvironment): DataSet[Int]= {
    env.fromElements(Gen.listOfN(numElements, g))
    val indexes : DataSet[(Int, Int)] = env.fromElements((1 to numPartitions):_*)
      .map(xs => (xs,xs)) //Create a tuple dataset. First element is partition number. Second is trivial now. Could be the seed in the future

    val finalDataSet: DataSet[Int] = indexes
        .partitionByRange(0) //Send each list to the partition with value equal to first position of the tuple
        .flatMap{_ =>
          val element: List[Int] = Gen.listOfN(numElements, g).sample.getOrElse(Nil)
          element
        }

    //print(finalDataSet.print())
    finalDataSet
  }


  def main(args: Array[String]): Unit = {
    //Intento de funcion que me piden
    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    val numPartitions = 4
    val numElements = 1000000
    val genVar = Gen.choose(1, 20) // -> Gen[Int]

    env.setParallelism(numPartitions) // Create a parallelism level equal to the num of partitions we want to create

    val gen_dataset : DataSet[Int] = generateDataSetGenerator(numElements, numPartitions, genVar)
    print(gen_dataset.collect().length)
  }
}
