import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalacheck.Arbitrary

val myGen = for {
  n <- Gen.choose(10, 20)
  m <- Gen.choose(2 * n, 500)
} yield (n, m)

val genList = Gen.listOfN(10, Gen.choose(50,100))

val propListLength = forAll(genList) { listaProp =>
  listaProp.forall{element:Int =>
    element >= 50 && element <=100
  }
}

propListLength.check()



//Prueba con Arbitrary
val anyInt = Arbitrary.arbitrary[Int]

val squares = for {
  xs <- Arbitrary.arbitrary[Seq[Int]]
} yield xs.map(x => x*x)

print(squares)



//Intento de funcion que me piden
var numElements = 1000
var genVar = Gen.choose(10, 20) // -> Gen[Int]

var seqSample = Gen.listOfN(1000, genVar).sample.get // -> Gen[Seq[Int]] Esto es una Seq o una list??? Segun docu es List: def listOfN[T](n: Int, g: Gen[T]): Gen[List[T]]

