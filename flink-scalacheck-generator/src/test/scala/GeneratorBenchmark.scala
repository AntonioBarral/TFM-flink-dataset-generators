import org.scalameter.api._
import org.scalacheck.Test
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalameter.Key

/** Object which calls different Benchmarks. Only run one at the same time */
object GeneratorBenchmark extends Bench.Group {
  //include(new SmallGeneratorBenchmark {})
  //include(new MediumGeneratorBenchmark {})
  //include(new HighGeneratorBenchmark {})
  //include(new HugeGeneratorBenchmark {})
  //include(new LinealEquation {})
  include(new QuadraticEquation {})
}

/**
 * Creates a Benchmark with a small range of elements
 */
trait SmallGeneratorBenchmark extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 10
  override val incrementElements = 50
  override val iterations = 30
  override val maxPartitions = 8
  override val rangePartitions = 2

  override implicit lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList

  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

    //A graph is created for each number of partitions to measure
      performance of "Graph with " + partitionNumber + " partition(s)" in {
        exec.independentSamples -> partitionNumber //Run in different JVM, regarding number of partitions

        val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
        val elementsNumber = for {
          elementNumber <- elementsGen
        } yield elementNumber

        //First, make some warmup for each JVM
        using(elementsNumber) config (
          exec.minWarmupRuns -> 5,
          exec.benchRuns -> 30
        ) in {
          value => propertyToSize(value/partitionNumber, partitionNumber).check(Test.Parameters.default.withMinSuccessfulTests(1))
        }
      }
    }
}


/**
 * Creates a Benchmark with a medium range of elements
 */
trait MediumGeneratorBenchmark extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 1000
  override val incrementElements = 5000
  override val iterations = 20
  override val maxPartitions = 8
  override val rangePartitions = 2

  override implicit lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList

  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

    //A graph is created for each number of partitions to measure
    performance of "Graph with " + partitionNumber + " partition(s)" in {
      exec.independentSamples -> partitionNumber //Run in different JVM, regarding number of partitions

      val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
      val elementsNumber = for {
        elementNumber <- elementsGen
      } yield elementNumber

      //First, make some warmup for each JVM
      using(elementsNumber) config (
        exec.minWarmupRuns -> 5,
        exec.benchRuns -> 30
      ) in {
        value => propertyToSize(value/partitionNumber, partitionNumber).check(Test.Parameters.default.withMinSuccessfulTests(1))
      }
    }
  }
}


/**
 * Creates a Benchmark with a high range of elements
 */
trait HighGeneratorBenchmark extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 100000
  override val incrementElements = 50000
  override val iterations = 20
  override val maxPartitions = 8
  override val rangePartitions = 2

  override implicit lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList

  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

    //A graph is created for each number of partitions to measure
    performance of "Graph with " + partitionNumber + " partition(s)" in {
      exec.independentSamples -> partitionNumber //Run in different JVM, regarding number of partitions

      val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
      val elementsNumber = for {
        elementNumber <- elementsGen
      } yield elementNumber

      //First, make some warmup for each JVM
      using(elementsNumber) config (
        exec.minWarmupRuns -> 2,
        exec.maxWarmupRuns -> 5,
        exec.benchRuns -> 30
      ) in {
        value => propertyToSize(value/partitionNumber, partitionNumber).check(Test.Parameters.default.withMinSuccessfulTests(1))
      }
    }
  }
}


/**
 * Creates a Benchmark with a huge range of elements
 */
trait HugeGeneratorBenchmark extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 1000000
  override val incrementElements = 2000000
  override val iterations = 5
  override val maxPartitions = 8
  override val rangePartitions = 4

  override implicit lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList

  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

    //A graph is created for each number of partitions to measure
    performance of "Graph with " + partitionNumber + " partition(s)" in {
      exec.independentSamples -> partitionNumber //Run in different JVM, regarding number of partitions

      val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
      val elementsNumber = for {
        elementNumber <- elementsGen
      } yield elementNumber

      //First, make some warmup for each JVM
      using(elementsNumber) config (
        exec.minWarmupRuns -> 2,
        exec.maxWarmupRuns -> 5,
        exec.benchRuns -> 30
      ) in {
        value => propertyToSize(value/partitionNumber, partitionNumber).check(Test.Parameters.default.withMinSuccessfulTests(1))
      }
    }
  }
}



/**
 * Creates a Benchmark to test a Flink program which executes a linear function
 */
trait LinealEquation extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 1000
  override val incrementElements = 5000
  override val iterations = 10
  override val maxPartitions = 8
  override val rangePartitions = 4

  override implicit lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList

  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

    //A graph is created for each number of partitions to measure
    performance of "Graph with " + partitionNumber + " partition(s)" in {
      exec.independentSamples -> partitionNumber //Run in different JVM, regarding number of partitions

      val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
      val elementsNumber = for {
        elementNumber <- elementsGen
      } yield elementNumber

      using(elementsNumber) config (
        exec.benchRuns -> 30
      ) in {
        value => linearFunct(getGameDataset(value/partitionNumber, partitionNumber), 2000)
      }
    }
  }
}


/**
 * Creates a Benchmark to test a Flink program which executes a quadratic function
 */
trait QuadraticEquation extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 1000
  override val incrementElements = 1000
  override val iterations = 10
  override val maxPartitions = 8
  override val rangePartitions = 4

  override implicit lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var listWorkers: List[Int] = (0 to maxPartitions by rangePartitions).toList

  listWorkers = listWorkers.updated(0,1)
  listWorkers.foreach { partitionNumber =>

    //A graph is created for each number of partitions to measure
    performance of "Graph with " + partitionNumber + " partition(s)" in {
      exec.independentSamples -> partitionNumber //Run in different JVM, regarding number of partitions

      val elementsGen = Gen.range("size")(initElements, incrementElements*iterations, incrementElements)
      val elementsNumber = for {
        elementNumber <- elementsGen
      } yield elementNumber

      using(elementsNumber) config (
        exec.minWarmupRuns -> 2,
        exec.maxWarmupRuns -> 5,
        exec.benchRuns -> 20
        ) in {
        value => quadraticFunct(getGameDataset(value/partitionNumber, partitionNumber), partitionNumber)
      }
    }
  }
}