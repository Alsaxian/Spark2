# Compte-rendu du second TP Spark

 
Auteur : Xian YANG  
Promotion : Data Science Math  
Date : 01 décembre 2017  

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## I. Partie principale : calcul des valeurs extrémales

Dans un premier temps, on travaille dans le contexte d'ignoration de la géométrie astronomique. En d'autres termes, on considère les RA et les Decl comme étant
des coordonnées cartésiennes orthogonales, où leur unités n'ont aucun lien l'une avec l'autre. Ce qui fait que lors du partitionnement en cases, on peut tranquillement 
partir du point de vu que la zone entière observée est un carré et la découper de façon équilibre dans les deux axes. Ce point de vue n'est pas tout à fait juste et se
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
un fichier. Le résultat est : 
```sh
$

```
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
et de nommer cette case de façon uniforme par ses ordres sur els deux axes.


