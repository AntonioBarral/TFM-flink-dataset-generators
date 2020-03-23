package flink_apps
import scala.reflect.ClassTag

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet

import org.apache.flink.streaming.api.scala._


object FlinkApps {

  /**
   * Generates a dataset tuple with each string passed by the DataSet[String] and its count
   * @param genDatasetSample
   * @return
   */
  def wordCount(genDatasetSample : DataSet[String]) (): Seq[(String, Int)] = {
    val counts  = genDatasetSample.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.collect()
  }
}




