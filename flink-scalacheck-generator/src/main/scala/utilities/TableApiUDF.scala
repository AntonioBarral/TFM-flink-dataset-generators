package utilities

import org.apache.flink.table.functions.ScalarFunction

/** Object to define UDF functions to Table API in Flink */
object TableApiUDF {
  class ToInt() extends ScalarFunction {
    def eval(s: String): Int = {
      s.toInt
    }
  }
}
