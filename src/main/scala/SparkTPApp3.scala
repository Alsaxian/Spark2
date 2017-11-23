import java.lang.Math.{max, min}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import schema.PetaSkySchema

object SparkTPApp3 {
  val compte = "p1715490" // changer avec votre login

  // Version avec une case class
  case class AP3Agg(objectId: String, count: Long, lastSrc: String,
                    minRA: Double, maxRA: Double,
                    minDecl: Double, maxDecl: Double) {
    def agg(b: AP3Agg): AP3Agg = {
      AP3Agg(objectId, count + b.count,
        if (lastSrc >= b.lastSrc) lastSrc else b.lastSrc,
        min(minRA, b.minRA), max(maxRA, b.maxRA),
        min(minDecl, b.minDecl), max(maxDecl, b.maxDecl)
      )
    }

    def toTuple: (String, Long, String, Double, Double, Double, Double) = {
      (objectId, count, lastSrc, minRA, maxRA, minDecl, maxDecl)
    }
  }

  def toAP3Agg(input: Array[String]): AP3Agg = {
    AP3Agg(input(PetaSkySchema.s_objectId), 1L, input(PetaSkySchema.s_sourceId),
      input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_ra).toDouble,
      input(PetaSkySchema.s_decl).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }

  def aggregateByObjectIdCC(inputDir: String, sc: SparkContext):
  RDD[(String, Long, String, Double, Double, Double, Double)] = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String)
      .map(toAP3Agg)
      .filter(_.objectId != "NULL")
      .map(a => (a.objectId, a))
      .reduceByKey((a, b) => a.agg(b))
      .map(r => r._2.toTuple)
  }

  // Version purement tuple
  def aggregateByObjectIdT(inputDir: String, sc: SparkContext):
  RDD[(String, Long, String, Double, Double, Double, Double)] = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String
      .filter(l => l(PetaSkySchema.s_objectId) != "NULL")
      .map(l => (l(PetaSkySchema.s_objectId), 1L, l(PetaSkySchema.s_sourceId),
        l(PetaSkySchema.s_ra).toDouble, l(PetaSkySchema.s_ra).toDouble,
        l(PetaSkySchema.s_decl).toDouble, l(PetaSkySchema.s_decl).toDouble)) // on extrait les attribus d'intérêt
      .map(l => (l._1, l))
      .reduceByKey((a, b) =>
        (a._1,
          a._2 + b._2,
          if (a._3 >= b._3) a._3 else b._3,
          min(a._4, b._4), max(a._5, b._5),
          min(a._6, b._6), max(a._7, b._7)))
      .map(r => r._2)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 2) {
      val conf = new SparkConf().setAppName("SparkTPApp3-" + compte)
      val sc = new SparkContext(conf)
      val result =
        if ("--tuple" == args(0))
          aggregateByObjectIdT(args(1), sc)
        else
          aggregateByObjectIdCC(args(1), sc)
      result
        .map(r => "%s,%d,%s,%s,%s,%s,%s".format(r._1, r._2, r._3, r._4, r._5, r._6, r._7))
        .saveAsTextFile(args(2))

    } else {
      println("Usage: spark-submit --class SparkTPApp3 /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "[ --caseClass | --tuple ] " +
        "hdfs:///user/" + compte + "/repertoire-donnees " +
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
  }
}
