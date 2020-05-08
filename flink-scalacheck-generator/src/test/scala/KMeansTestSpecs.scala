import flink_apps.KMeans
import generator.Generator

import flink_apps.KMeans.{Centroid, Point}

import org.apache.flink.api.scala._
import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

/**
 * Class to test DataSet Gen in KMeans example from Apache Flink github example located in [[flink_apps.KMeans]]
 */
class KMeansTestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest {
  sequential

  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  override val partitions: Int = 3
  override val elements: Int = 100
  override val seed: Int = 0

  /**
   * Creates a Point DataSet Gen using coordinates within the provided range
   * @param xMin min x
   * @param xMax max x
   * @param yMin min y
   * @param yMax max y
   * @return Point DataSet Gen
   */
  def createPointsGenerator(xMin: Int, xMax: Int, yMin: Int, yMax: Int): Gen[DataSet[Point]] = {
    val genPoint: Gen[Point] = for {
      x <- Gen.choose(xMin, xMax)
      y <- Gen.choose(yMin, yMax)
    } yield Point(x, y)

    Generator.generateDataSetGenerator(elements, partitions, genPoint)
  }

  private val initialCentroidGen = for {
    x <- Gen.choose(-100, 100)
    y <- Gen.choose(-100, 100)
  } yield  Centroid(1, x, y)

  private val numCentroidsGen = Gen.choose(2,5)
  private val iterationsGen = Gen.choose(10,20)
  private val centroidsDistanceGen = Gen.choose(5, 20)


  "This property checks that initial centroids points assign in a line belongs to the same class, after KMeans algorithm" >>
    Prop.forAll(initialCentroidGen, numCentroidsGen, iterationsGen, centroidsDistanceGen) {
      (initialCentroid: Centroid, numCentroids: Int, iterations: Int, centroidsDistance: Int) =>

        val pointsInRadiusGen: Gen[Int] = Gen.choose(1, centroidsDistance/2)
        var centroids: List[Centroid] = List(initialCentroid)

        //Fill points DataSet for the initial centroid
        var pointsDataSet: DataSet[Point] =
          createPointsGenerator(
            initialCentroid.x.toInt - pointsInRadiusGen.sample.get, initialCentroid.x.toInt + pointsInRadiusGen.sample.get,
            initialCentroid.y.toInt - pointsInRadiusGen.sample.get, initialCentroid.y.toInt + pointsInRadiusGen.sample.get
          ).sample.get

        //Fill list of centroids by multiplying its id by centroidsDistance, and then add initialCentroid value for each coordinate
        for (i <- 1 to numCentroids) {
          val currentCentroid = Centroid(i + 1, (i * centroidsDistance) + initialCentroid.x, (i * centroidsDistance) + initialCentroid.y)
          centroids :+= currentCentroid

          //create elements * partitions number of points per centroid applying a range using pointsInRadiusGen
          val xMin = currentCentroid.x.toInt - pointsInRadiusGen.sample.get
          val xMax = currentCentroid.x.toInt + pointsInRadiusGen.sample.get
          val yMin = currentCentroid.y.toInt - pointsInRadiusGen.sample.get
          val yMax = currentCentroid.y.toInt + pointsInRadiusGen.sample.get

          pointsDataSet = pointsDataSet.union(createPointsGenerator(xMin, xMax, yMin, yMax).sample.get)

        }

        // Set centroidsDataset and add centroids to pointsDataset
        val centroidsDataSet: DataSet[Centroid] = env.fromCollection(centroids)
        pointsDataSet = pointsDataSet.union(centroidsDataSet.map(xs => Point(xs.x, xs.y)))

        val result = KMeans.kMeansCalc(pointsDataSet, centroidsDataSet, iterations)
        val clusteredPoints: DataSet[(Int, Point)] = result._1

        //Get the points which were the initialCentroids
        val initialPoints: List[Centroid] = clusteredPoints
          .distinct()
          .filter(xs => centroids.contains(Centroid(xs._1, xs._2.x, xs._2.y)))
          .map(xs => Centroid(xs._1, xs._2.x, xs._2.y))
          .collect().toList

        initialPoints must containTheSameElementsAs(centroids)

    }.set(minTestsOk = 100)
}
