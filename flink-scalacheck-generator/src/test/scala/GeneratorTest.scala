import org.apache.flink.api.scala.ExecutionEnvironment

trait GeneratorTest {

  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val partitions: Int
  val elements: Int
  val seed: Int

}
