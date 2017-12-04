/**
  * Created by xian on 04.12.17.
  */


import SparkTPApp7ExtNew.{Grille, calculMaxMin, dansQuellesCasesVaisJe, reformerEnPairRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
class SparkTPApp7ExtNewTest extends FunSuite with BeforeAndAfter  {
  var sc: SparkContext = null;

  // exécuter avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTPApp3Test")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("Vérifie maxMin"){
    val resultat = SparkTPApp7ExtNew.calculMaxMin("samples/source-sample", sc)
    println(resultat.misEnChaineCarac)
  }

  test("dansQuellesCasesVaisJe") {
    val maGrille = Grille(5, calculMaxMin("samples/source-sample", sc))
    maGrille.Cases.map(_.nom).map(println)

    dansQuellesCasesVaisJe(-2.00, 2.67, maGrille).map(_._1).map(println)
    val RDDaEcrire = reformerEnPairRDD("samples/source-sample", sc, maGrille)
    RDDaEcrire.map(_._1).map(println)
  }


  // exécuter après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}

