# Compte-rendu du second TP Spark

 
Autheur : Xian YANG
Promotion : Data Science Math
Date : 01 décembre 2017

 &ensp; &ensp; 
 &ensp; &ensp; 
  
## I. Partie principale 1 : calcul des valeurs extrémales

Dans un premier temps, on travaille dans le contexte d'ignoration de la géométrie astronomique. En d'autres termes, on considère les RA et les Decl comme étant
des coordonnées cartésiennes orthogonales, où leur unités n'ont aucun lien l'une avec l'autre. Ce qui fait que lors du partitionnement en cases, on peut tranquillement 
partir du point de vu que la zone entière observée est un carré et la découper de façon équilibre dans les deux axes. Ce point de vue n'est pas tout à fait juste et se
verra améliorer dans la partie III.  
  
Le premier but de ce TP est de calculer les valeurs extrémales de la zone entière observée. Pour le faire, une idée en parallélisme est de regrouper les sources en zones 
puis les zones en plus grandes etc. Ainsi, il convient de définir d'abord une `case classe Zone`
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
qui contient une méthode pour faire de la `reduce` dans le futur et une autre méthode pour renvoyer les valeurs extrémales.  
  
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

