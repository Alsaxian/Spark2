import java.lang.Math.{max, min}
import org.apache.spark.{SparkConf, SparkContext}
import schema.PetaSkySchema

/**
  * Created by xian on 15.11.17.
  */

object SparkTPApp4Zone {
  val compte = "p1715490"

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

  def convertirEnZone (input: Array[String]): Zone = {
    Zone(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_ra).toDouble,
      input(PetaSkySchema.s_decl).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }

  def calculMaxMin(inputDir: String, sc: SparkContext):
  Zone = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String)
      .map(convertirEnZone)
      .reduce(_ zone_englobante _)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 2) {
      val conf = new SparkConf().setAppName("SparkTPApp4Zone-" + compte)
      val sc = new SparkContext(conf)
      val result =
        if ("--tuple" == args(0))
          "What ?"
        else
          calculMaxMin(args(1), sc).misEnChaineCarac
      println(result)
    } else {
      println("Usage: spark-submit --class SparkTPApp4Zone /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "[ --caseClass | --tuple ] " +
        "hdfs:///user/" + compte + "/repertoire-donnees " +
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
  }

}
