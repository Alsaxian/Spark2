import org.apache.spark.{SparkConf, SparkContext}

object SparkTPApp1 {

  val compte = "ecoquery"

  def compteAB(logFile: String, sc: SparkContext): (Long, Long) = {
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    (numAs, numBs)
  }

  def main(args: Array[String]): Unit = {
    val logFile = "hdfs:///user/" + compte + "/README.md"
    val conf = new SparkConf().setAppName("SparkTPApp1-" + compte)
    val sc = new SparkContext(conf)
    val (nbA, nbB) = compteAB(logFile, sc)
    println("\n\nLines with a: %s, Lines with b: %s\n\n".format(nbA, nbB))
  }

}
