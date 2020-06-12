import java.util.Calendar

import GeneratorBenchmarkTrait.Game
import generator.Generator
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.scalacheck.{Gen, Prop}

object GeneratorBenchmarkTrait{
  case class Game(name: String, gender: String, launchYear: Int, pegi: Int, score: Double)

}

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


  def lineadFunct(gameDataset: DataSet[Game], filterYear: Int): List[String] = {
    val result = gameDataset
      .filter(game => game.launchYear >= filterYear)
      .map(game => game.name).collect()

    result.toList
  }

  /*def cuadraticFunct(gameDataset: DataSet[Game]): List[(String, String, Double)] = {
    val result = gameDataset
        .groupBy(xs => (xs.pegi, xs.gender))
        .reduceGroup  {
          (in, out: Collector[(String, String, Double)]) =>
            var total = 0.0
            var pegi = 0
            var gender = ""
            var number = 0

            in.foreach({ xs =>
              total +=  xs.score
              pegi = xs.pegi
              gender = xs.gender
              number += 1
            })

            out.collect(("PEGI " + pegi, "Gender " + gender, total/number))
        }.collect().toList
    result
  }*/

  def getGameDataset(elementsPerPartition: Int, partitions: Int): DataSet[Game] = {
    val gameGen = for {
      name <- Gen.alphaStr
      gender <- Gen.oneOf("Action", "Adventure", "Visual novel", "RPG", "MOBA", "Shooter", "MMORPG", "Platforms", "Puzzles", "Simulator")
      launchYear <- Gen.choose(1980, Calendar.getInstance.get(Calendar.YEAR))
      pegi <- Gen.oneOf(3, 7, 12, 16, 18)
      score <- Gen.choose(0.0, 100.0).map(xs => Math.round(xs * 100.0) / 100.0)
    } yield Game(name, gender, launchYear, pegi, score)

    Generator.generateDataSetGenerator(elementsPerPartition, partitions, gameGen).sample.get

  }
}
