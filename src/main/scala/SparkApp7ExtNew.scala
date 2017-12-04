import java.lang.Math.{max, min}

import org.apache.spark.SparkContext
import schema.PetaSkySchema

/**
  * Created by xian on 04.12.17.
  */
object SparkApp7ExtNew {
  val compte = "p1715490"

  val proximiteHorizontale = 0.1
  val proximiteVerticale = 0.1


  class Zone(minRA: Double, maxRA: Double,
                  minDecl: Double, maxDecl: Double) {
    def zone_englobante(b: Zone): Zone = {
      new Zone(min(minRA, b.minRA), max(maxRA, b.maxRA),
        min(minDecl, b.minDecl), max(maxDecl, b.maxDecl))
    }

    def misEnChaineCarac: String = {
      "minRA : %f, maxRA : %f, minDecl : %f, maxDecl : %f"
        .format(minRA, maxRA, minDecl, maxDecl)
    }
  }

  def convertirEnZone (input: Array[String]): Zone = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    val decl = input(PetaSkySchema.s_decl).toDouble
    new Zone(if(ra > 180) -ra else ra, if(ra > 180) -ra else ra, decl, decl)
  }

  def calculMaxMin(inputDir: String, sc: SparkContext):
  Zone = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String)
      .map(convertirEnZone)
      .reduce(_ zone_englobante _)
  }

  class Case(leftRA: Double, rightRA: Double,
             upperDecl: Double, lowerDecl: Double) {
    var compteur: Int = 0
    def verifierAppartenance (ra: Double, decl: Double): {

    }
  }
}
