/**
  * Created by Xian on 2017/12/1.
  */

import SparkTPApp6Extensions.dansQuellesCasesVaisJe
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
class SparkTPApp6ExtentionsTest1 extends FunSuite with BeforeAndAfter  {
  var sc: SparkContext = null;

  // exécuter avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTPApp3Test")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("Cases ok ?") {
    dansQuellesCasesVaisJe(1, 3.0).foreach(println)
    dansQuellesCasesVaisJe(359, 3.0).foreach(println)
    dansQuellesCasesVaisJe(8, 3.0).foreach(println)
    dansQuellesCasesVaisJe(17, 3.0).foreach(println)
    dansQuellesCasesVaisJe(351, 3.0).foreach(println)
    dansQuellesCasesVaisJe(10, 3.0).foreach(println)
    dansQuellesCasesVaisJe(11, 3.0).foreach(println)
    dansQuellesCasesVaisJe(111, 3.0).foreach(println)
    dansQuellesCasesVaisJe(211, 3.0).foreach(println)
  }

  // exécuter après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }

}
