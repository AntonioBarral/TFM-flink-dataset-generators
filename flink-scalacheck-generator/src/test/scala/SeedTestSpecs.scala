import scala.io.Source

class SeedTestSpecs extends org.specs2.mutable.Specification {
  private val filesPath = "/home/antonio/TFM/Code/TFM-flink-generators/flink-scalacheck-generator/src/test/resources/"
  private val attempts = 3
  private val workers = 3
  private val date = "2020-03-07_15:14:38"
  private var workerValues: Map[Int, List[String]] = Map()

  for (worker <- 0 to workers-1) {
    workerValues += (worker -> List.empty[String])
    for (attempt <- 0 to attempts-1) {

      val values  = {
        val src = Source.fromFile(filesPath + date + "_" + "worker_" + worker + "_attempt_" + attempt + ".txt")
        val line = src.getLines.take(1).toList
        src.close
        line
      }

      workerValues = workerValues.updated(worker, workerValues.get(worker).get ++ values)

    }
  }

  s2"""
 Each task has created the same data in each attempt $dataByWorker
 The whole dataset is equal in each attempt $datasetByAttempt
"""

  def dataByWorker = {
    /* Map structure:
    {
       worker1: List("1234567","1234567","1234567"),
       worker2: List("98765", "98765", "98765"),
       ...
    }*/
    var areEqual = true
    workerValues.foreach({
      keyVal =>
        if (!keyVal._2.forall(_ == keyVal._2.head)) {
          areEqual = false
        }

    })
    areEqual must_== true
  }

  def datasetByAttempt = {
    var attemptValues = List("", "", "")
    workerValues.foreach({
      keyVal =>
        for ((element, index) <- keyVal._2.zipWithIndex) {
          attemptValues = attemptValues.updated(index, attemptValues(index) + element)
        }
    })

    attemptValues.forall(_ == attemptValues.head) must_== true
  }
}