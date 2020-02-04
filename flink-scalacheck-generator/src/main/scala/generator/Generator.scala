package generator

import org.apache.flink.api.scala._
import org.scalacheck.Gen


object Generator {

  def generateSeqGenerator (numElements: Int, numPartitions: Int, g: Seq[Int]): Gen[Seq[Int]] = {
    var seqGen = Gen.listOfN()

  }

  /*def generateDatasetGenerator(numElements: Int, numPartitions: Int, g: Gen[T]): Gen[DataSet[T]] = {


  }*/

  def main(args: Array[String]): Unit = {
    val numPartitions = 4
    val numElements = 100000
    val sequence = Seq(1,2,3,4,5,6,7,8,9)
  }
}
