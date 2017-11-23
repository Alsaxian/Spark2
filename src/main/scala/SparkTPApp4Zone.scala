import java.lang.Math.{max, min}

//import org.apache.spark.rdd.RDD
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
      //.filter(_./*anything*/ != "NULL")
      .reduce(_ zone_englobante _)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 2) {
      val conf = new SparkConf().setAppName("SparkTPApp4Zone-" + compte)
      val sc = new SparkContext(conf)
      val result =
        if ("--tuple" == args(0))
          "What ?"//aggregateByObjectIdT(args(1), sc)
        else
          calculMaxMin(args(1), sc).misEnChaineCarac
      println(result)
        //.map(r => "%s,%d,%s,%s,%s,%s,%s".format(r._1, r._2, r._3, r._4, r._5, r._6, r._7))
        //.saveAsTextFile(args(2))

    } else {
      println("Usage: spark-submit --class SparkTPApp4Zone /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "[ --caseClass | --tuple ] " +
        "hdfs:///user/" + compte + "/repertoire-donnees " +
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
  }

}
