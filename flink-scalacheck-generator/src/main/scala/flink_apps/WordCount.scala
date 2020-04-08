package flink_apps
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala._


object WordCount {

  /**
   * Generates a dataset tuple with each string passed by the DataSet[String] and its count
   * @param genDatasetSample
   * @return collect from words counted
   */
  def wordCountCalc(genDatasetSample : DataSet[String]): Seq[(String, Int)] = {
    val counts  = genDatasetSample.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.collect()
  }

}




