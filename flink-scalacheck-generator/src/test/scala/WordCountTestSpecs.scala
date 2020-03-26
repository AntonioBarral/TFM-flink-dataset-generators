import flink_apps.FlinkApps
import generator.Generator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.scalacheck.{Gen, Prop, Properties}
import org.specs2.ScalaCheck
import org.specs2.matcher.ResultMatchers

import scala.collection.mutable.ListBuffer

class WordCountTestSpecs extends org.specs2.mutable.Specification with ScalaCheck with ResultMatchers with GeneratorTest  {

  implicit val typeInfo: TypeInformation[String] = TypeInformation.of(classOf[String])
  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


  override val partitions: Int = 3
  override val elements: Int = 5
  override val seed = 0

  val valueDatasets = List("antonio", "enrique", "juan")
  val numDatasets = valueDatasets.size

  var genDatasets: ListBuffer[Gen[DataSet[String]]] = ListBuffer.empty[Gen[DataSet[String]]] //Dataset with generator and word generated


  /**
   * Creates a generator with a Gen.const of that value to generate a dataset of len elements with the same value
   *
   */
   def createGenerator(value: String): Gen[DataSet[String]] = {
   Generator.generateDataSetGenerator(elements, partitions, Gen.const(value))

  }

  for (index <- 0 until numDatasets) {
    genDatasets :+= createGenerator(valueDatasets(index))
  }

  val genDatasetAntonio: Gen[DataSet[String]] = createGenerator("antonio")
  val genDatasetEnrique: Gen[DataSet[String]] = createGenerator("enrique")
  val genDatasetJuan: Gen[DataSet[String]] = createGenerator("juan")



  //ScalaCheck test

  val p2: Properties = new Properties("Count total elements in word count") {
    property("EqualNumberOfElements") = Prop.forAll(genDatasetAntonio, genDatasetEnrique, genDatasetJuan){
      (d1: DataSet[String], d2: DataSet[String], d3: DataSet[String]) =>
        val totalCount = d1.count() + d2.count() + d3.count()
        val dataset = d1.union(d2.union(d3))
        //println(FlinkApps.wordCount(dataset).map{case (_,t2) => t2}.sum + "------" +totalCount)
        FlinkApps.wordCount(dataset).map{case (_,t2) => t2}.sum must_== totalCount

    }
    property("NotEqualNumberOfElements") = Prop.forAll(genDatasetAntonio, genDatasetEnrique, genDatasetJuan){
      (d1: DataSet[String], d2: DataSet[String], d3: DataSet[String]) =>

        val totalCount = d1.count() + d2.count() + d3.count()
        val dataset = d1.union(d2.union(d3))
        //println(FlinkApps.wordCount(dataset).map{case (_,t2) => t2}.sum + "------" +totalCount)
        FlinkApps.wordCount(dataset).map{case (_,t2) => t2}.sum must_!=  totalCount -10
        
    }
  }
  s2"addition and multiplication are related $p2"
}
