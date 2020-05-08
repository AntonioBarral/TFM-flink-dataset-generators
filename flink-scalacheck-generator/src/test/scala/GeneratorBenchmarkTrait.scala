import generator.Generator
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.scalacheck.{Gen, Prop}

/** Trait for use it in every type of benchmark done with ScalaMeter */
trait GeneratorBenchmarkTrait {
  val initElements: Int
  val incrementElements: Int
  val iterations: Int
  val maxPartitions: Int
  val rangePartitions: Int

  lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment //Need to be a lazy val to prevent serialization error

  /**
   * Small test creating a dataset generator to use it in the ScalaMeter Benchmark traits
   * @param elementsPerPartition Number of elements assigned for every partition
   * @param partitions Number of partitions used to create a dataset generator
   * @return Property to check
   */
  def propertyToSize(elementsPerPartition: Int, partitions: Int): Prop = {
    val testGen = Gen.choose(1,elementsPerPartition)
    val dGen: Gen[DataSet[Int]] = Generator.generateDataSetGenerator(elementsPerPartition, partitions, testGen)
    Prop.forAll(dGen) {
      d: DataSet[Int] => {
        d.distinct().count() <= d.count()/partitions
      }
    }
  }
}
