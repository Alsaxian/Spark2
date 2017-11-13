import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object TIW6RDDUtils {

  /**
    * Classe reprise de  https://stackoverflow.com/questions/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job
    * Elle permet de déterminer un nom de fichier en fonction d'une clé pour chaque valeur à écrire
    */
  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]
  }

  /**
    * Ecrit le contenu d'un RDD consitué d'éléments qui sont des paires (clé,valeur) dans différent fichiers.
    * Le nom du fichier ou chaque valeur est écrite est donné par la clé.
    *
    * @param rddToWrite le rddAEcrire dans HDFS
    * @param dir        le répertoire qui contiendra les fichiers pour chaque case
    * @param nbCases    le nombre approximatif de cases à écrire
    */
  def writePairRDDToHadoopUsingKeyAsFileName(rddToWrite: RDD[(String, String)], dir: String, nbCases: Int): Unit = {
    rddToWrite
      .partitionBy(new HashPartitioner(nbCases))
      .saveAsHadoopFile(dir, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
  }
}
