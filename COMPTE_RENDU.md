# Compte-rendu du second TP Spark

 
Auteurs : AUJOGUE JEAN-BAPTISTE p0400503 et YANG XIAN p1715490  
Promotion : Data Science Math  
Date : 01 décembre 2017  
Lien du rapport :
[https://forge.univ-lyon1.fr/p1715490/tp-spark2-2017/blob/master/COMPTE_RENDU.md](https://forge.univ-lyon1.fr/p1715490/tp-spark2-2017/blob/master/COMPTE_RENDU.md)

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## I. Partie principale : calcul des valeurs extrémales

Dans un premier temps, on travaille dans le contexte d'ignoration de la géométrie astronomique. En d'autres termes, on considère les RA et les Decl comme étant
des coordonnées cartésiennes orthogonales, où leur unités n'ont aucun lien l'une avec l'autre. Ce qui fait que lors du partitionnement en cases, on peut tranquillement 
partir du point de vue que la zone entière observée est un carré et la découper de façon équilibre dans les deux axes. Ce point de vue n'est pas tout à fait juste et se
verra améliorer dans la partie III.  
  
Le premier but de ce TP est de calculer les valeurs extrémales de la zone entière observée. Pour le faire, une idée en parallélisme est de regrouper 
les sources en zones puis les zones en plus grandes etc. Ainsi, il convient de définir d'abord une `case classe Zone` dans le 4ème fichier source `SparkTPApp4Zone.scala`
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
```scala
  def main(args: Array[String]): Unit = {
    if (args.length >= 3) {
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
        "hdfs:///user/" + compte + "/repertoire-donnees")
    }
  }
```

__Attention : comme le serveur était en panne ce dernier temps, il ne me permet même plus de réexécuter cette partie du programme sur un seul fichier de 80 Mo !
Donc je ne peux pas donner de résultat exact ici, mais dans mon mémoire, lorsque je l'avais exécuté il y a très longtemps où le serveur marchait encore, j'avais comme
résultat que le min des RA est un peu plus que 0, le max des RA un peu moins que 360 et que le min des Decl est un peu plus que -7, le min des RA un peu moins que 7.__

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## II. Partie principale : Mise en grille de la zone et partitionnement des sources
Maintenant on s'intéresse au découpage de la grande zone observée en petites cases, afin de partitionner les sources de manière que chaque fichier de sortie ne dépasse
pas 128 Mo. Etant donné qu'on en a 5 Go en total, il nous faut donc au moins (5 / 0,128 approx.=) 40 cases. A noter qu'ici, on demande à l'utilisateur
de saisir ce nombre à clavier au moment de la soumise de requête, tout dans l'eprit de lui donner la liberté de géstion de la taille des fichiers. 
(Ce ne sera plus le cas dans la partie suivante. ) Comme expliqué plus haut, on considère pour le moment que la zone entière a pour forme un carré, dont on découpe
les deux côté de façon identique. Ainsi, dans le 5ème fichier source `SparkTPApp5Partitionnement.scala`, la `class Grille` suivante
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
  
__Conclusion : En comparant l'histogramme du fichier `002` avec celui du fichier `062`, on voit que la distribution des observations dans le premier est plutôt 
uniforme alors que celle dans le dernier est très variée. Ceci peut être dû au fait que la zone étudiée n'est pas, comme on l'imaginait, une bande étroite 
(0, 360) * (-4, 4). Mais en effet, ça peut être une petite zone où l'intervalle des RA dépasse la "ligne de changement de date" (RA = 0) ! Du coup, dans le premier 
fichier les données sont situées uniquement d'un seul côté de cette ligne d'où une distribution uniforme, tant dis que dans le dernier fichier c'est le cas contraire,
d'où beaucoup de vide dans l'espace "au milieu".__

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## III. Partie extensions
Dans cette partie, on va répondre aux 3 questions :  
  
1. Quelle est la vraie géométrie astronomique est comment rendre notre partitionnement plus pertinent à cette géométrie ?
2. Comment définir "être proche d'une case voisine" d'un point et comment l'affecter à plusieurs cases dans un contexte RDD ?
3. Partage des charges - quand une case est trop remplie, que faire ?

### 1. Quelle est la vraie géométrie astronomique est comment rendre notre partitionnement plus pertinent à cette géométrie ?
La première partie de cette question est facile à répondre. Le système de coordonnées formé par RA et Decl c'est comme le shpère du globe :
RA prend sa valeur dans [0, 360) mais quand il dépasse 360 ça recommence à 0. Decl prend sa valeur dans [-90, 90].  
  
Du coup, quand dans ce TP les valeurs extrémales de RA sont approximitivement resp. 0 et 360, ça nous a fait croire en premier lieu que la 
zone observée est une bande étroite qui entourne le sphère. Cependant, après avoir parcouru une grande quntité de fichiers originaux (oui !
parce que le serveur est en panne !) fichier par fichier, nous commençons à nous rendre compte que la zone observée est en effet un petit
"rectangle" plus ou moins centré à l'origine, puisque les RA, s'ils sont grands, ne decendent jamais en dessous de 358 et s'ils sont petits,
ne montent jamais au dessus de 2 !! C'est bien une piège qu'il faut évider lors du traitement.  
  
Pour constater ce que nous pensons, la solution simple est de mettre les grands RA en opposés. Plus concrètement, une fois qu'une valeur de RA 
dépasse 180, on lui enlève 360. Du coup les RA ne résident après ce traitement que dans l'intervalle [-2, 2] et le partitionnement sera
beaucoup plus compact qu'avant (plus de cases vides en plein milieu). 
Dans notre codes, il y a trois endroitsqu'il faut modifier, notamment à la lecture et à l'écriture 
des valeurs des observations.
```scala
  def convertirEnZone(input: Array[String]): Zone = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    val decl = input(PetaSkySchema.s_decl).toDouble
    Zone(if (ra > 180) ra-360 else ra, if (ra > 180) ra-360 else ra, decl, decl)
  }
  
    def garderSeulementRaDecl (input: Array[String]): Array[Double] = {
    val ra = input(PetaSkySchema.s_ra).toDouble
    Array(if (ra > 180) ra-360 else ra, input(PetaSkySchema.s_decl).toDouble)
  }

  def dansQuellesCasesVaisJe (ra: Double, decl: Double, grille: Grille):
  List[(String, String)] = {
    grille.Cases.filter(verifierAppartenance(grille, _, ra, decl)).map(_.nom)
      .map((_, (if (ra < 0) ra+360 else ra).toString + "," + decl.toString)).toList
  }
```
A noter qu'à la sortie, si un RA est négatif, on lui rend la somme de 360.

### 2. Comment définir "être proche d'une case voisine" d'un point et comment l'affecter à plusieurs cases dans un contexte RDD ?
Il comvient de définir d'abord deux distances
```scala
  val proximiteHorizontale = 0.1
  val proximiteVerticale = 0.1
```

Si une observation est plus proche de la frontière avec une autre case que l'une de ces distances,
elle doit alors appartenir à cette case aussi. 
```scala
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
```
On verra des fichiers appelé "bis" quand on exécutera le script sur un fichier de 80 Mo sur le serveur.  
  
La difficulté d'effectuer une telle affectation d'un point à plusieurs cases, dans l'environnement Spark,  
consiste principalement en ceci : à la sortie,
une valeur d'observation peut avoir plusieurs clés. Donc ici on doit faire recours à la structure `pairRDD` est appliquer la méthode
`flatMap`.
```scala
  def reformerEnPairRDD (inputDir: String, sc: SparkContext, grille: Grille)
  : RDD[(String, String)] =
    sc.textFile(inputDir)
      .map(_.split(",").map(_.trim))
      .map(garderSeulementRaDecl)
      .flatMap(arr => dansQuellesCasesVaisJe(arr(0),arr(1), grille))
```
Cette méthode peut, grossièrement dit, "aplatir une liste de listes" en une liste. Du coup, quand une observation appartient à 
plusieurs cases, elle forme une liste dont chaque élément est composé d'une clé différente est de sa valeur. La méthode `flatMap`
fait en sorte à décomposer cette liste est rendre chacun de ses éléments en tant qu'élément de la nouvelle PairRDD.


### 3. Partage des charges - quand une case est trop remplie, que faire ?
C'est la partie la plus fatigante du TP. Ici on prend le chemin le plus direct : on pose un seuil à chacune des cases, p. ex. 
```scala
  val maxObsDansUneCase = 10000
```
et met à sa disposition un compteur qui compte le nombre d'observations qui tombent dans elle. Une fois ce seuil atteint, on lui 
affetera plus rien, même si les coordonnées d'un futur point peut convenir. Ensuite une nouvelle case des mêmes bords sera automatiquement
construite et c'est elle qui prendra les observations dans son royaume.  
  
Avant, dans le fichier source `SparkTPApp5Partitionnement.scala` on n'a pas construit une scala `class` pour la case et on a simplement
comparer chaque observation avec la grille en faisant une boucle `for` emboîtée. Or ici, évidemment la case joue un rôle beaucoup plus
indépendant de manière qu'il mérite une telle structure. Juste pour nommer une raison, la création d'une nouvelle case, avec notre 
technique de simple comparaison des observations avec deux séquences comme avant, n'est plus possible. 
```scala
  case class Case(leftRA: Double, rightRA: Double,
             upperDecl: Double, lowerDecl: Double, nom: String) {
    var compteur: Int = 0
  }
```
En plus, la `class Grille` doit également devenir plus compliquer qui joue maintenant un rôle de "gestionnaire de cases".
```scala
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
```
A noter que une structure `mutable` de case (ici : `ListBuffer`) est nécessaire, qui nous permettra d'y en concatener des nouvelles. 
```scala
  def ajouterCase (grille: Grille, leftRA: Double, rightRA: Double,
                  upperDecl: Double, lowerDecl: Double, nom: String): Unit = {
    grille.Cases += Case(leftRA, rightRA, upperDecl, lowerDecl, nom)
  }
```
Une copie complète de cette dernière version de code est annexée à la fin du rapport.  
   
Maintenant un histogramme de distribution d'observations, est beaucoup plus uniforme. On liste ici les histogramme des mêmes fichiers originaux 
`002` et `062` pour comparer  
`002` : 
```scala
5.2-->****
6.3-->**
4.2-->****
4.4-->****
6.0-->**
2.3-->***
4.5-->****
5.6-->****
3.1-->****
3.4-->****
5.3-->****
6.4-->**
3.0-->****
4.1-->****
2.4-->***
3.5-->****
2.0-->***
4.6-->****
4.0-->****
3.3-->****
2.6-->***
6.1-->**
6.5-->**
2.1-->***
5.1-->****
3.2-->****
2.5-->***
5.5-->****
3.6-->****
6.2-->**
4.3-->****
5.0-->****
6.6-->**
2.2-->***
5.4-->****
```
`062` : 
```scala
0.1-->****
0.5bis-->****
0.5-->****
0.3bis-->****
0.0-->****
0.2bis-->****
0.4-->****
0.4bis-->****
0.3-->****
0.6bis-->****
0.0bis-->****
0.1bis-->****
0.6-->****
0.2-->****
```
On voit que premièrement, les nombres d'observations à la sortie ne dépasse pas 10.000, comme on le souhaitait. Deuxièmement, pour le fichier `062`, des `bis` 
apparaissent à la sortie, signifiant la création de nouvelles cases au fur et à mesure.

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## Partie IV. Difficulté rencontrée dans ce TP
C'est surtout que le serveur ne marchait pas pour les données entières !! Ceci a entraîné ce qu'on a cru pendant longtemps que la zone
observée est une bande de 360 degré, puisque c'était difficile de vérifier dans l'intégralité. Du coup on a fait une autre version de
programme pour les questions de l'extension basé sur ce fait. Mais au dernier moment où on se rendait compte de la "piège", il fallait tout 
refaire.  
  
D'ailleurs on a constaté un phénomène intéressant : en Spark il faut plutôt d'écrire des `case class`, que d'écrire des `class` et de
les instancier plus tard avec un `new`. Lorsqu'on a pris la deuxième manière au début, le compileur nou signalait que c'était une 
structure "non-`serializable`" et refuse d'exécuter le script. Bien que même d'après le bouquin 
[Programming in Scala](https://www.amazon.fr/Programming-Scala-Martin-Odersky/dp/0981531644) de 
[Martin Odersky](https://fr.wikipedia.org/wiki/Martin_Odersky), sauf besoin de l'utilisation de `pattern matching`, ces deux structures
ne présentent aucune différence.

```scala
import java.lang.Math._

import TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName

import scala.collection.mutable.{Buffer, ListBuffer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import schema.PetaSkySchema

import scala.collection.immutable.List


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
```





