/**
  * Created by Xian on 2017/12/1.
  */






import scala.collection.immutable.List
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import schema.PetaSkySchema

import TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName

object SparkTPApp6Extensions {

  val compte = "p1715490"

  /* Stratégie :

   */

  val nbFichiersMin: Int = 40

  /* Que doit-on comprendre par ce "qu'un point est proche d'une case voisine" ? Ici sans précision dans l'énoncé, on va dire
  qu'un point est proche et donc va être inclus dans une case voisine, s'il se situe à moins de 1 degré de la frontière.
   */
  val proximite = 1

  /* Dans un premier temps, en tenant compte de la forme géométrique de la zone entière à observer,
  on a toute raison de ne découper que les ascensions droites qui fait un tour de 360 degrés.
  */
  val longueurCase = 360/nbFichiersMin
  val grilleRA: Range = 0 to 360 by longueurCase


  def dansQuellesCasesVaisJe (ra: Double, decl: Double): List[(String, String)] = {
    var i:Int = 0

    while (ra > grilleRA(i)) {
      i += 1
    }
    /* Une fois la première bonne case trouvée, ce qui est la valeur de i,
    on regarde si le point est proche d'une case voisine.
     */
    /* Ici si on veut passer de List[Any] à List[Int], il faut vraiment mettre "else" à chaque branche. Parce que sinon
    la valeur retournée est "unit", alors (), ce qui n'est pas de type Int.
     */
    val noCases: List[Any] = i :: {
      if (i == 1) { // cas particulier, 1ère case
        if (ra <= (i - 1) * longueurCase + proximite) // proche de RA == 0 ?
          nbFichiersMin
        else if (ra >= i * longueurCase - proximite) // proche de RA == 9 ?
          i + 1
      } else if (i == nbFichiersMin) { // cas particulier, dernière case
        if (ra <= (i - 1) * longueurCase + proximite)
          i - 1
        else if (ra >= 360 - proximite) // proche de RA == 0 ?
          1
      } else {
        if (ra <= (i - 1) * longueurCase + proximite) // cas ordinaire
          i - 1
        else if (ra >= i * longueurCase - proximite)
          i + 1
      }
    } :: Nil

    noCases.filter(_ != ()).map(_.toString).map((_, ra.toString + "," + decl.toString))
  }

  def garderSeulementRaDecl (input: Array[String]): Array[Double] = {
    Array(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }

  /*def raffinerMaCase (input:)*/
  def reformerEnPairRDD (inputDir: String, sc: SparkContext)
  : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String)
      .map(garderSeulementRaDecl)
      .flatMap(arr => dansQuellesCasesVaisJe(arr(0),arr(1)))



  def main(args: Array[String]): Unit = {

    if (args.length >= 3) {
      val conf = new SparkConf().setAppName("SparkTPApp6Extensions-" + compte)
      val sc = new SparkContext(conf)


      val inputDir = args(0)
      val outputDir = args(1)


      val RDDaEcrire = reformerEnPairRDD(inputDir, sc)

      writePairRDDToHadoopUsingKeyAsFileName(RDDaEcrire, outputDir,
        nbFichiersMin)
    }



    else {
      println("Usage: spark-submit --class SparkTPApp5Partitionnement /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "hdfs:///user/" + compte + "/repertoire-donnees " + /* half useful for the moment */
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
  }
}
