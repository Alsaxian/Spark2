# Compte-rendu du second TP Spark

 
Auteur : Xian YANG  
Promotion : Data Science Math  
Date : 01 décembre 2017  

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## I. Partie principale : calcul des valeurs extrémales

Dans un premier temps, on travaille dans le contexte d'ignoration de la géométrie astronomique. En d'autres termes, on considère les RA et les Decl comme étant
des coordonnées cartésiennes orthogonales, où leur unités n'ont aucun lien l'une avec l'autre. Ce qui fait que lors du partitionnement en cases, on peut tranquillement 
partir du point de vue que la zone entière observée est un carré et la découper de façon équilibre dans les deux axes. Ce point de vue n'est pas tout à fait juste et se
verra améliorer dans la partie III.  
  
Le premier but de ce TP est de calculer les valeurs extrémales de la zone entière observée. Pour le faire, une idée en parallélisme est de regrouper 
les sources en zones puis les zones en plus grandes etc. Ainsi, il convient de définir d'abord une `case classe Zone`
```scala
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
```
qui contient une méthode pour faire de la `reduce` dans le futur et une autre méthode pour renvoyer les valeurs extrémales en chaîne de caractères.  
  
Ensuite la fonction `convertirEnZone` permet de initialiser le problème, de façon à convertir un point sous forme d'une `Zone` en répétant deux fois ses coordonnées.
```scala
  def convertirEnZone (input: Array[String]): Zone = {
    Zone(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_ra).toDouble,
      input(PetaSkySchema.s_decl).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }
```
Finalement, une `map` sert à convertir tous les points en `Zone` suivi d'une `reduce` qui sert à les regrouper petit à petit jusqu'à une seule.
```scala
  def calculMaxMin(inputDir: String, sc: SparkContext):
  Zone = {
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) 
      .map(convertirEnZone)
      .reduce(_ zone_englobante _)
  }
```
Pour effectuer une vérification intermédiaire on pourrait choisir de mettre une méthode `main` qui affiche à l'écran ces quatre valeurs obtenues où les stocke dans 
un fichier. 

__Attention : comme le serveur était en panne ce dernier temps, il ne me permet même plus de réexécuter cette partie du programme sur un seul fichier de 80 Mo !
Donc je ne peux pas donner de résultat exact ici, mais dans mon mémoire, lorsque je l'avais exécuté il y a très longtemps où le serveur marchait encore, j'avais comme
résultat les RA entre environ 0 et 360 et les Decl entre environ -4 et 4.__

## II. Partie principale : Mise en grille de la zone et partitionnement des sources
Maintenant on s'intéresse au découpage de la grande zone observée en petites cases, afin de partitionner les sources de manière que chaque fichier de sortie ne dépasse
pas 128 Mo. Etant donné qu'on en a 5 Go en total, il nous faut donc au moins (5 / 0,128 approx.=) 40 cases. A noter qu'ici, on demande à l'utilisateur
de saisir ce nombre à clavier au moment de la soumise de requête, tout dans l'eprit de lui donner la liberté de géstion de la taille des fichiers. 
(Ce ne sera plus le cas dans la partie suivante. ) Comme expliqué plus haut, on considère pour le moment que la zone entière a pour forme un carré, dont on découpe
les deux côté de façon identique. Ainsi, la `class Grille` suivante
```scala
  case class Grille (nbMinCases: Int, limites: Zone) {
    val nbCasesRacineCarre: Int =
      if (nbMinCases < 0 || nbMinCases > 26 * 26) {
        println("Try again!")
        1 
      } else
        ceil(sqrt(nbMinCases)).toInt 

    val grilleRA: NumericRange[Double] = limites.minRA to limites.maxRA by
      (limites.maxRA - limites.minRA) / nbCasesRacineCarre

    val grilleDecl: NumericRange[Double] = limites.minDecl to limites.maxDecl by
      (limites.maxDecl - limites.minDecl) / nbCasesRacineCarre
  }
```
va découper la zone en des cases de 7*7 avec un nombre minimum de cases égal à 40, 
qui ne renvoie pas vraiment des cases en tant qu'objets de scala mais plutôt deux partitionnements
de côtés, pour raison d'efficacité de calcul (sinon, on devra comparer les sources avec chaque case pour trouver enfin sa "maison").  
  
Ensuite la fonction `dansQuelleCaseVaisJe` permet de décider la bonne case pour une source en fonction de ses coordonnées
```scala
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
```
et de nommer les cases de façon uniforme par leurs ordres sur les deux axes, p. ex. "2.3".  
  
On a également besoin d'une fonction qui lit une ligne d'observation brute et en extraire les valeurs RA et Decl
```scala
  def garderSeulementRaDecl (input: Array[String]): Array[Double] = {
    Array(input(PetaSkySchema.s_ra).toDouble, input(PetaSkySchema.s_decl).toDouble)
  }
```
et finalement d'une fonction qui rassemble toutes celles-là en faisant du MapReduce et en retournant un fichier par case à la sortie, 
grâce à la fonction d'écriture `writePairRDDToHadoopUsingKeyAsFileName`, fournie dans le fichier source `TIW6RDDUtils`.
```scala
  def reformerEnPairRDD (inputDir: String, sc: SparkContext, grille: Grille)
    : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String)
      .map(garderSeulementRaDecl)
      .map(arr => (dansQuelleCaseVaisJe(arr(0),arr(1),grille), arr(0).toString + "," + arr(1).toString))
```
A remarquer ici qu'on a dû transformer chaque enregistrement en un n-uplet scala de longueur 2, dont le premier élément est la clé, i.g.
le nom de la case à laquelle il appartient.  
  

Au final on écrit une méthode main qui appelle la fonction `reformerEnPairRDD` et résume la taille (en nombre de lignes) de chaque fichier
à la sortie, à travers un histogramme.
```scala
  def main(args: Array[String]): Unit = {

    if (args.length >= 3) {
      val conf = new SparkConf().setAppName("SparkTPApp5Partitionnement-" + compte)
      val sc = new SparkContext(conf)

      val nbMinCases = args(0).toInt
      val inputDir = args(1)
      val outputDir = args(2)

      val maGrille = Grille(nbMinCases, calculMaxMin(inputDir, sc))
      val RDDaEcrire = reformerEnPairRDD(inputDir, sc, maGrille)

      writePairRDDToHadoopUsingKeyAsFileName(RDDaEcrire, outputDir,
        maGrille.nbCasesRacineCarre * maGrille.nbCasesRacineCarre)

      RDDaEcrire.countByKey().foreach {case (key, value) => println (key + "-->" +
        "*" * ceil(log10(value)).toInt)}
    }

    else {
      println("Usage: spark-submit --class SparkTPApp5Partitionnement /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "nbMinCases " +
        "hdfs:///user/" + compte + "/repertoire-donnees " + /* half useful for the moment */
        "hdfs:///user/" + compte + "/repertoire-resultat")
    }
```
A cause de la panne du serveur de Spark, je n'ai pas pu obtenir le résultat sur l'ensemble des 5 Go d'observation. En revanche j'ai réussi à exécuter ce script 
sur des fichiers seuls. Au final, pour les fichiers `002` et `062` les histogrammes sont respectivement comme suit  
`002` :
```sh
6.3-->****
4.4-->****
3.7-->****
7.3-->****
4.5-->****
5.6-->****
3.4-->****
6.4-->****
0.0-->
2.7-->****
7.2-->*****
2.4-->****
3.5-->****
1.6-->****
5.7-->****
1.5-->****
4.6-->****
1.4-->****
2.6-->****
6.1-->****
7.1-->****
6.5-->
1.7-->****
2.5-->****
5.5-->****
3.6-->****
7.4-->****
6.2-->****
7.0-->
6.6-->***
4.7-->****
5.4-->***
```
`062` : 
```sh
1.2-->****
7.3-->***
0.0-->
7.2-->*****
1.6-->****
7.5-->*****
7.1-->****
1.7-->*****
1.3-->*
7.0-->
```
Ici, le numéro devant la flèche est le nom du fichier, le nombre d'étoile représente le nombre de ligne du fichier de sortie pris de logarithme à la base 10.
Le cas avec aucune étoile signifie qu'il y a une seule ligne dans le fichier.

## III. Partie extensions
