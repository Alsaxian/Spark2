import SparkTPApp5Partitionnement.{Grille, reformerEnPairRDD, garderSeulementRaDecl, dansQuelleCaseVaisJe}
import SparkTPApp4Zone.Zone
import TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.io.Source

/**
  * Created by xian on 22.11.17.
  */
class SparkApp5PartitionnementTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = null;

  // exécuter avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTPApp3Test")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }


  test("Vérifie la version case class avec source-sample") {

    val maGrille = Grille(3, Zone(357.954210, 358.090417, 2.564629, 3.175271))
    maGrille.grilleRA.foreach(println)
    val inputDir = "samples/source-sample"
    val outputDir = "brouillon"

    val caseOuJeVais: String = dansQuelleCaseVaisJe(358.08, 2.94, maGrille)
    println(caseOuJeVais) // Donc la fonction dansQuelleCaseVaisJe n'a pas de problème.

    val veriGarderSeulement = garderSeulementRaDecl(Source.fromFile(inputDir).getLines.next().split(","))
    veriGarderSeulement.foreach(println) // Donc la fonction garderSeulementRaDecl n'a pas de problème.
    println(dansQuelleCaseVaisJe(veriGarderSeulement(0),veriGarderSeulement(1),maGrille))


    val RDDaEcrire = reformerEnPairRDD(inputDir, sc, maGrille)
    println(RDDaEcrire.getClass)
    println(maGrille.nbCasesRacineCarre * maGrille.nbCasesRacineCarre)
    /*writePairRDDToHadoopUsingKeyAsFileName(RDDaEcrire, outputDir,
      maGrille.nbCasesRacineCarre * maGrille.nbCasesRacineCarre)*/
  }

  // exécuter après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }

}
