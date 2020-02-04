package generator

import org.apache.flink.api.scala._
import org.scalacheck.Gen


object Generator {

  def generateDatasetGenerator(numElements: Int, numPartitions: Int, g: Gen[Int]): Gen[DataSet[Int]] = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //env.fromCollection(Seq(1,2,3,4,5,6))
    env.fromCollection(Gen.listOfN(numElements, g))
    env.setParallelism(numPartitions)
  }

  def main(args: Array[String]): Unit = {
    //Intento de funcion que me piden
    val numPartitions = 4
    val numElements = 100000
    val genVar = Gen.choose(1, 200000) // -> Gen[Int]

    generateDatasetGenerator(numElements, numPartitions, genVar)
  }
}
