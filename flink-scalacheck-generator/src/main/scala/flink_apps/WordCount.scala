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

  def wordCountTableCalc(tableSample: Table, desired_frequency: Long)(implicit tEnv: BatchTableEnvironment): DataSet[WC] = {

    val result = tableSample
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === desired_frequency)
      .toDataSet[WC]

    result

  }

  case class WC(word: String, frequency: Long)
}



