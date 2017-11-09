import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkTPApp3Test extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTPApp3Test")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  def checkResult(result: RDD[(String,Long,String,Double,Double,Double,Double)]): Any = {
    result.cache()
    assert(result.count() == 1)
    val agg = result.collect()(0)
    assert(agg._1 == "430209694171136")
    assert(agg._2 == 6)
    assert(agg._3 == "29809086638981597")
    assert((agg._4 - 357.9893337673741).abs < 0.00000000000001 )
    assert((agg._5 - 357.9894411124743).abs < 0.00000000000001 )
    assert((agg._6 - 2.5646291352701804).abs < 0.00000000000001 )
    assert((agg._7 - 2.564762701468148).abs < 0.00000000000001 )
  }

  test("Vérifie la version tuple avec source-sample") {
    val rdd = SparkTPApp3.aggregateByObjectIdT("samples/source-sample", sc)
    checkResult(rdd)
  }


  test("Vérifie la version case class avec source-sample") {
    val rdd = SparkTPApp3.aggregateByObjectIdCC("samples/source-sample", sc)
    checkResult(rdd)
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
