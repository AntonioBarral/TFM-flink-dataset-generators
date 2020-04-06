package utilities

import org.apache.flink.table.functions.ScalarFunction

object TableApiUDF {
  class ToInt() extends ScalarFunction {
    def eval(s: String): Int = {
      s.toInt
    }
  }
}
