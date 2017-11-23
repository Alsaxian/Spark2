import java.lang.Math.{ceil, max, min, sqrt}

import SparkTPApp4Zone.{Zone, calculMaxMin}


import scala.collection.immutable.NumericRange
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import schema.PetaSkySchema

import TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName


/**
  * Created by xian on 15.11.17.
  */
object SparkTPApp5Partitionnement {

  val compte = "p1715490"

/*  class Case(minRA: Double, maxRA: Double, minDecl: Double, maxDecl: Double, val nom: String)
    extends Zone(minRA, maxRA, minDecl, maxDecl)*/



  case class Grille (nbMinCases: Int, limites: Zone) {
    val nbCasesRacineCarre: Int =
      if (nbMinCases < 0 || nbMinCases > 26 * 26) {
        println("Try again!")
        1 // A remplir par un message d'exception!
      } else
        ceil(sqrt(nbMinCases)).toInt // à améliorer avec ifelse

    val grilleRA: NumericRange[Double] = limites.minRA to limites.maxRA by
      (limites.maxRA - limites.minRA) / nbCasesRacineCarre

    val grilleDecl: NumericRange[Double] = limites.minDecl to limites.maxDecl by
      (limites.maxDecl - limites.minDecl) / nbCasesRacineCarre

/*    val CasesRetournees: Array[Case] =
      for ((ra, i) <- grilleRA.zipWithIndex)
        for ((decl, j) <- grilleDecl.zipWithIndex)
          yield new Case()*/
  }

  def dansQuelleCaseVaisJe (ra: Double, decl: Double, grille: Grille): String = {
    var i:Int = 0
    var j:Int = 0

    while (ra > grille.grilleRA(i)) {
      while (decl > grille.grilleDecl(j))
        j += 1
      i += 1
    }

    i.toString + "." + j.toString
  }

  def garderSeulementRaDecl (input: Array[String]): Array[Double] = {
    Array(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }


  def reformerEnPairRDD (inputDir: String, sc: SparkContext, grille: Grille)
    : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String)
      .map(garderSeulementRaDecl)
      .map(arr => (dansQuelleCaseVaisJe(arr(0),arr(1),grille), arr(0).toString + "," + arr(1).toString))


  def main(args: Array[String]): Unit = {

    if (args.length >= 4) {
      val conf = new SparkConf().setAppName("SparkTPApp5Partitionnement-" + compte)
      val sc = new SparkContext(conf)

      val nbMinCases = args(1).toInt
      //val limitesEnArray = args(2).split(",").map(_.trim.toDouble)
      val inputDir = args(2)
      val outputDir = args(3)

      /*val maGrille = Grille(nbMinCases, Zone(limitesEnArray(0), limitesEnArray(1),
        limitesEnArray(2), limitesEnArray(3)))*/
      val maGrille = Grille(nbMinCases, calculMaxMin(inputDir, sc))
      val RDDaEcrire = reformerEnPairRDD(inputDir, sc, maGrille)

      writePairRDDToHadoopUsingKeyAsFileName(RDDaEcrire, outputDir,
        maGrille.nbCasesRacineCarre * maGrille.nbCasesRacineCarre)
    }



    else {
      println("Usage: spark-submit --class SparkTPApp5Partitionnement /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "nbMinCases" +
        "MinMaxRaDeclSeparesParVirgule" +
        "[ --caseClass | --tuple ] " +
        "hdfs:///user/" + compte + "/repertoire-donnees " + /* half useful for the moment */
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
  }
}

