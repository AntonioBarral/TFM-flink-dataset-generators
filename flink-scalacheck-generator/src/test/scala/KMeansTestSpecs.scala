import flink_apps.KMeans
import generator.Generator
import flink_apps.KMeans.{Centroid, Point}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

class KMeansTestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest {

  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  override val partitions: Int = 3
  override val elements: Int = 100
  override val seed: Int = 0

  private val random = scala.util.Random

  def createPointsGenerator(): Gen[DataSet[Point]] = {
    val genPoint: Gen[Point] = for {
      x <- Gen.chooseNum(-100.0, 100.0).map(xs => Math.round(xs * 100.0) / 100.0)
      y <- Gen.chooseNum(-100.0, 100.0).map(xs => Math.round(xs * 100.0) / 100.0)
    } yield Point(x, y)

    Generator.generateDataSetGenerator(elements, partitions, genPoint)
  }

  //Test 1
  private val pointsGen: Gen[DataSet[Point]] = createPointsGenerator()
  private val numCentroidsRnd = Gen.choose(2,5)
  private val iterationsRnd = Gen.choose(10,20)

  "This property checks that initial centroids vary from the final ones" >>
    Prop.forAll(pointsGen, numCentroidsRnd, iterationsRnd) {
      (pointsDataset: DataSet[Point], numCentroids: Int, iterations: Int) =>
      println(numCentroids, iterations)
      val centroidsDataset: DataSet[Centroid] = pointsDataset
        .first(numCentroids)
        .zipWithUniqueId
        .map(xs => Centroid(xs._1 toInt, xs._2.x, xs._2.y))

      val result = KMeans.kMeansCalc(pointsDataset, centroidsDataset, iterations)
      val finalCentroids = result._2

      finalCentroids.collect() must_!= containTheSameElementsAs(centroidsDataset.collect())

    }.set(minTestsOk = 100)
}
