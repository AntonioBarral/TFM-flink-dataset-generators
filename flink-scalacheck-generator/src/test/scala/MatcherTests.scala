import es.ucm.fdi.sscheck.matcher.specs2.flink
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

class MatcherTests extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest{
  sequential

  override implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  override val partitions: Int = 0
  override val elements: Int = 0
  override val seed: Int = 0
  val emptyDataSet = env.fromCollection[Int](Nil)
  val nonEmptyDataSet = env.fromElements(0)

  "Checking beSubDataSetOf and beEqualDataSetTo" >> {
    val d1 = env.fromElements(2,3,3)
    val d2 = env.fromElements(3,2,2)
    val d3 = env.fromElements(2,3,2)
    val xs = env.fromCollection(1 to 10)
    val ys = env.fromCollection(5 to 15)
    val zs = env.fromCollection(1 to 5)

    println("Starting tests for beSubDataSetOf and beEqualDatasetTo")
    emptyDataSet must flink.DataSetMatchers.beEqualDataSetTo(emptyDataSet)
    println("1--------------------------------------")
    emptyDataSet must flink.DataSetMatchers.beSubDataSetOf(xs)
    println("2--------------------------------------")
    xs must flink.DataSetMatchers.nonBeEqualDataSetTo(emptyDataSet)
    println("3--------------------------------------")
    zs must flink.DataSetMatchers.beSubDataSetOf(xs)
    println("4--------------------------------------")
    xs must flink.DataSetMatchers.beSubDataSetOf(xs)
    println("5--------------------------------------")
    emptyDataSet must flink.DataSetMatchers.beSubDataSetOf(xs)
    println("6--------------------------------------")
    xs must flink.DataSetMatchers.nonBeEqualDataSetTo(emptyDataSet)
    println("7--------------------------------------")
    xs must flink.DataSetMatchers.nonBeSubDataSetOf(ys)
    println("8--------------------------------------")
    ys must flink.DataSetMatchers.nonBeSubDataSetOf(xs)
    println("9--------------------------------------")
    d1 must flink.DataSetMatchers.nonBeEqualDataSetTo(d2)
    println("10--------------------------------------")
    d2 must flink.DataSetMatchers.nonBeEqualDataSetTo(d1)
    println("11--------------------------------------")
    d3 must flink.DataSetMatchers.beEqualDataSetTo(d2)
    println("12--------------------------------------")
    d2 must flink.DataSetMatchers.beEqualDataSetTo(d3)
    println("13--------------------------------------")
    d1 must flink.DataSetMatchers.beEqualDataSetTo(d2, strict = false)
    println("14--------------------------------------")
    d2 must flink.DataSetMatchers.beEqualDataSetTo(d1, strict = false)
    println("15--------------------------------------")
    d3 must flink.DataSetMatchers.beEqualDataSetTo(d2, strict = false)
    println("16--------------------------------------")
    d2 must flink.DataSetMatchers.beEqualDataSetTo(d3, strict = false)
    println("17--------------------------------------")
    println("Done tests for beSubDataSetOf and beEqualDatasetTo")
    ok
  }
}
