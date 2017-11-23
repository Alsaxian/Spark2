import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkTPApp1Test extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTPApp1Test")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("compte a et b") {
    val (nA, nB) = SparkTPApp1.compteAB("README.md", sc)

    println("\n nA est : " + nA + " et nB est : " + nB)
    assert(nA == 53, ", mauvais nombre de a") // change avec README.md
    assert(nB == 23, ", mauvais nombre de b")
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
