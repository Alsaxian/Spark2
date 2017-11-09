import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkTPApp2Test extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTPApp2Test")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("compte sample-source") {
    val countRDD = SparkTPApp2.countByObjectId("samples/source-sample", sc)
    val countById = countRDD.collectAsMap()
    assert(countById("430209694171136") == 6)
    assert(countById.size == 1)
    assert(! countById.contains("NULL"))
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
