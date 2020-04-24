package flink_apps
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object WordCount {

  /**
   * Generates a dataset tuple with each string passed by the DataSet[String] and its count
   * @param dataSetSample
   * @return collect from words counted
   */
  def wordCountDataSetCalc(dataSetSample : DataSet[String]): Seq[(String, Int)] = {
    val counts  = dataSetSample.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.collect()
  }

  def wordCountTableCalc(tableSample: Table, frequency: Long)(implicit tEnv: BatchTableEnvironment): DataSet[WC] = {

    val result = tableSample
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === frequency)
      .toDataSet[WC]

    //println(tEnv.toDataSet[WC](tableSample).collect())
    result

  }

  case class WC(word: String, frequency: Long)
}



