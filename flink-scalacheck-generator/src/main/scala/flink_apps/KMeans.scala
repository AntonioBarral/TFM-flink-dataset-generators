package flink_apps

/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * K-Means is an iterative clustering algorithm and works as follows:
 * K-Means is given a set of data points to be clustered and an initial set of ''K'' cluster
 * centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (''mean'') of all points that have
 * been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.
 * This is the Wikipedia entry for the [[http://en.wikipedia
 * .org/wiki/K-means_clustering K-Means Clustering algorithm]].
 *
 * This implementation works on two-dimensional data points.
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * Input files are plain text files and must be formatted as follows:
 *
 * - Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.
 * For example `"1.2 2.3\n5.3 7.2\n"` gives two data points (x=1.2, y=2.3) and (x=5.3,
 * y=7.2).
 * - Cluster centers are represented by an integer id and a point value.
 * For example `"1 6.2 3.2\n2 2.9 5.7\n"` gives two centers (id=1, x=6.2,
 * y=3.2) and (id=2, x=2.9, y=5.7).
 *
 * Usage:
 * {{{
 *   KMeans --points <path> --centroids <path> --output <path> --iterations <n>
 * }}}
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.clustering.util.KMeansData]]
 * and 10 iterations.
 *
 * This example shows how to use:
 *
 * - Bulk iterations
 * - Broadcast variables in bulk iterations
 * - Scala case classes
 */

object KMeans {

  def kMeansCalc(points: DataSet[Point], centroids: DataSet[Centroid], iterations: Int): (DataSet[(Int, Point)], DataSet[Centroid]) = {

    val finalCentroids = centroids.iterate(iterations) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }.withForwardedFields("_1; _2")
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2.add(p2._2), p1._3 + p2._3) }.withForwardedFields("_1")
        .map { x => new Centroid(x._1, x._2.div(x._3)) }.withForwardedFields("_1->id")
      newCentroids
    }

    val clusteredPoints: DataSet[(Int, Point)] =
      points.map(new SelectNearestCenter).withBroadcastSet(finalCentroids, "centroids")

    (clusteredPoints, finalCentroids)

  }


  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
   * Common trait for operations supported by both points and centroids
   * Note: case class inheritance is not allowed in Scala
   */
  trait Coordinate extends Serializable {

    var x: Long
    var y: Long

    def add(other: Coordinate): this.type = {
      x += other.x
      y += other.y
      this
    }

    def div(other: Long): this.type = {
      x /= other
      y /= other
      this
    }

    def euclideanDistance(other: Coordinate): Double =
      Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y))

    def clear(): Unit = {
      x = 0
      y = 0
    }

    override def toString: String =
      s"$x $y"

  }

  /**
   * A simple two-dimensional point.
   */
  case class Point(var x: Long = 0, var y: Long = 0) extends Coordinate with Ordered [Point] {
    override def compare(other: Point): Int = {
      if (this.x == other.x && this.y == other.y)
        0
      else if (this.x > other.x || (this.x == other.x && this.y > other.y))
        1
      else
        -1
    }
  }

  /**
   * A simple two-dimensional centroid, basically a point with an ID.
   */
  case class Centroid(var id: Int = 0, var x: Long = 0, var y: Long = 0) extends Coordinate with Ordered[Centroid] {

    def this(id: Int, p: Point) {
      this(id, p.x, p.y)
    }

    override def toString: String =
      s"$id ${super.toString}"

    override def compare(other: Centroid): Int = {
      if (this.id == other.id)
        0
      else if (this.id > other.id)
        1
      else
        -1
    }
  }

  /** Determines the closest cluster center for a data point. */
  @ForwardedFields(Array("*->_2"))
  final class SelectNearestCenter extends RichMapFunction[Point, (Int, Point)] {
    private var centroids: Traversable[Centroid] = null

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }

    def map(p: Point): (Int, Point) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for (centroid <- centroids) {
        val distance = p.euclideanDistance(centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = centroid.id
        }
      }
      (closestCentroidId, p)
    }

  }
}
