import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

val myGen = for {
  n <- Gen.choose(10, 20)
  m <- Gen.choose(2 * n, 500)
} yield (n, m)

val lista = Gen.listOfN(10, Gen.choose(50,100))

val propListLength = forAll(lista) { listaProp =>
  listaProp.forall{element:Int =>
    element >= 50 && element <=100
  }
}

propListLength.check()