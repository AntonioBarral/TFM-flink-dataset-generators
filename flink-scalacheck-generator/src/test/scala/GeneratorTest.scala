import org.apache.flink.api.scala.ExecutionEnvironment

/** Trait used for every ScalaCheck test done using specs2 with flink examples tests */
trait GeneratorTest {

  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val partitions: Int
  val elements: Int
  val seed: Int

}
