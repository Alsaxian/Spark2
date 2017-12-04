import java.lang.Math._

import TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName

import scala.collection.mutable.{Buffer, ListBuffer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import schema.PetaSkySchema

import scala.collection.immutable.List


/**
  * Created by xian on 04.12.17.
  */
object SparkTPApp7ExtNew {
  val compte = "p1715490"

  val proximiteHorizontale = 0.1
  val proximiteVerticale = 0.1
  val maxObsDansUneCase = 10000

  case class Zone(minRA: Double, maxRA: Double,
             minDecl: Double, maxDecl: Double) {
    def zone_englobante(b: Zone): Zone = {
      Zone(min(minRA, b.minRA), max(maxRA, b.maxRA),
        min(minDecl, b.minDecl), max(maxDecl, b.maxDecl))
    }

    def misEnChaineCarac: String = {
      "minRA : %f, maxRA : %f, minDecl : %f, maxDecl : %f"
        .format(minRA, maxRA, minDecl, maxDecl)
    }
  }

  def convertirEnZone(input: Array[String]): Zone = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    val decl = input(PetaSkySchema.s_decl).toDouble
    Zone(if (ra > 180) ra-360 else ra, if (ra > 180) ra-360 else ra, decl, decl)
  }

  def calculMaxMin(inputDir: String, sc: SparkContext):
  Zone = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim))
      .map(convertirEnZone)
      .reduce(_ zone_englobante _)
  }

  case class Case(leftRA: Double, rightRA: Double,
             upperDecl: Double, lowerDecl: Double, nom: String) {
    var compteur: Int = 0
  }

  case class Grille (nbMinCases: Int, limites: Zone) {
    val nbCasesRacineCarre: Int =
      if (nbMinCases < 0 || nbMinCases > 26 * 26) {
        println("Try again!")
        1
      } else
        ceil(sqrt(nbMinCases)).toInt

    val longueurCase = (limites.maxRA - limites.minRA) / nbCasesRacineCarre
    val hauteurCase = (limites.maxDecl - limites.minDecl) / nbCasesRacineCarre

    var Cases = new ListBuffer[Case]

    for (i <- 0 until nbCasesRacineCarre; j <- 0 until nbCasesRacineCarre)
      Cases += Case(limites.minRA + i * longueurCase,
        limites.minRA + (i+1) * longueurCase,
        limites.minDecl + i * hauteurCase, limites.minDecl + (i+1) * hauteurCase,
        i.toString + "." + j.toString)
  }

  def ajouterCase (grille: Grille, leftRA: Double, rightRA: Double,
                  upperDecl: Double, lowerDecl: Double, nom: String): Unit = {
    grille.Cases += Case(leftRA, rightRA, upperDecl, lowerDecl, nom)
  }

  def verifierAppartenance (grille: Grille, casa: Case,
                            ra: Double, decl: Double): Boolean = {
    val assertion = ra >= casa.leftRA - proximiteHorizontale &&
      ra <= casa.rightRA + proximiteHorizontale &&
      decl >= casa.upperDecl - proximiteVerticale &&
      decl <= casa.lowerDecl + proximiteVerticale &&
      casa.compteur < maxObsDansUneCase

    if (assertion) {
      casa.compteur += 1
      if (casa.compteur == maxObsDansUneCase)
        ajouterCase(grille, casa.leftRA, casa.rightRA, casa.upperDecl, casa.lowerDecl,
          casa.nom + "bis")
    }
    assertion
  }

  def dansQuellesCasesVaisJe (ra: Double, decl: Double, grille: Grille):
  List[(String, String)] = {
    grille.Cases.filter(verifierAppartenance(grille, _, ra, decl)).map(_.nom)
      .map((_, (if (ra < 0) ra+360 else ra).toString + "," + decl.toString)).toList
  }

  def garderSeulementRaDecl (input: Array[String]): Array[Double] = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    Array(if (ra > 180) ra-360 else ra, input(PetaSkySchema.s_decl).toDouble)
  }

  def reformerEnPairRDD (inputDir: String, sc: SparkContext, grille: Grille)
  : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim))
      .map(garderSeulementRaDecl)
      .flatMap(arr => dansQuellesCasesVaisJe(arr(0),arr(1), grille))


  def main(args: Array[String]): Unit = {

    if (args.length >= 3) {
      val conf = new SparkConf().setAppName("SparkTPApp6Extensions-" + compte)
      val sc = new SparkContext(conf)

      val nbFichiersMin = args(0).toInt
      val inputDir = args(1)
      val outputDir = args(2)

      val maGrille = Grille(nbFichiersMin, calculMaxMin(inputDir, sc))
      val RDDaEcrire = reformerEnPairRDD(inputDir, sc, maGrille)

      writePairRDDToHadoopUsingKeyAsFileName(RDDaEcrire, outputDir,
        nbFichiersMin)

      RDDaEcrire.countByKey().foreach {case (key, value) => println (key + "-->" +
        "*" * ceil(log10(value)).toInt)}
    }



    else {
      println("Usage: spark-submit --class SparkTPApp5Partitionnement /home/" +
        compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "40 " +
        "hdfs:///user/" + compte + "/repertoire-donnees " +
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
  }
}