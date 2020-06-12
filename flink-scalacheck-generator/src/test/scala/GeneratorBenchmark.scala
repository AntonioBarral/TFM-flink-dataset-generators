import org.scalameter.api._
import org.scalacheck.Test
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalameter.Key

/** Object which calls different Benchmarks. Only run one at the same time */
object GeneratorBenchmark extends Bench.Group {
  private final val mainPath = "/home/antonio/Desktop/"

  Key.reports.resultDir -> mainPath + "SmallGeneratorBenchmark"
  //include(new SmallGeneratorBenchmark {})

  Key.reports.resultDir -> mainPath + "MediumGeneratorBenchmark"
  //include(new MediumGeneratorBenchmark {})

  Key.reports.resultDir -> mainPath + "HighGeneratorBenchmark"
  //include(new HighGeneratorBenchmark {})

  Key.reports.resultDir -> mainPath + "HugeGeneratorBenchmark"
  //include(new HugeGeneratorBenchmark {})

  include(new LinealEquation {})
}

/**
 * Creates a Benchmark with a small range of elements
 */
trait SmallGeneratorBenchmark extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 10
  override val incrementElements = 50
  override val iterations = 10
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
 * Creates a Benchmark with a medium range of elements
 */
trait MediumGeneratorBenchmark extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 1000
  override val incrementElements = 5000
  override val iterations = 10
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
 * Creates a Benchmark with a high range of elements
 */
trait LinealEquation extends Bench.OfflineReport with GeneratorBenchmarkTrait {

  override val initElements = 100
  override val incrementElements = 500
  override val iterations = 10
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
        exec.benchRuns -> 30
      ) in {
        value => lineadFunct(getGameDataset(value/partitionNumber, partitionNumber), 2000)
      }
    }
  }
}