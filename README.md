# Spark: agrégation et partitionnement

L'objectif de ce TP est de pousser un peu plus loin l'utilisation de Spark.
Le code fourni dans ce TP correspond au premier TP Spark, jusqu'à la troisième application Spark (Calcul de valeurs extrémales) inclue.

> Ce TP manipulant éventuellement de grosses données il faut:
> * Supprimer les répertoires HDFS inutiles: `hdfs dfs -rm -r repertoire-a-supprimer`
> * Lancer ensuite `hdfs dfs -expunge` pour vider régulièrement la poubelle HDFS

L'utilitaire [`screen`](https://en.wikipedia.org/wiki/GNU_Screen) permet (entre autres) de conserver un shell actif tout en se déconnectant ([quickref](http://aperiodic.net/screen/quick_reference)).
Il ne faut pas hésiter à l'utiliser quand on lance des jobs longs sur le cluster. 

Le [projet contenant cet énoncé](https://forge.univ-lyon1.fr/EMMANUEL.COQUERY/tp-spark2-2017) peut être utilisé comme point de départ.
Il reprend en partie le TP précédent.

## Rendu

Ce TP est à rendre pour le **19/11/2017**.
Il se fera de préférence en binôme.
Il est demandé de déposer un fichier `.zip` sur tomuss dans la case `TP_SPARK2`.
Cette archive comprendra:
* Le code source
* Le fichier `etudiants.yml` complété
* Le fichier `COMPTE_RENDU.md` complété avec en particulier:
    * Les étudiants du binôme (repris depuis le fichier `etudiants.yml`) 
    * les problèmes qui se sont posés et les solutions apportées
    * les extraits pertinents du code source
    
Le TP jusqu'au programme de partitionnement initial et son application aux données complètes sera notée sur 14 points, les extensions pourront augmenter la note à raison de 3 points max par extension (20 max au total).
Il est demandé de privilégier un rendu fonctionnel (_i.e._ qui s'exécute sans erreur) à l'ajout de fonctionnalités supplémentaires.

## Problème posé

On souhaite pouvoir comparer des observations (sources) avec d'autres observations proches en termes de coordonnées (i.e. une distance `ra,decl` inférieure à une certaine valeur).
Cette opération étant initialement coûteuse (quadratique) si on s'y prend de manière naïve, on souhaite partitionner les données pour optimiser les calculs postérieurs.
L'idée est de colocaliser les observations appartenant à une même région du ciel afin de limiter les recherches futures à cette région (technique dite de _blocking_).

## Blocs HDFS et grille de coordonnées

Le système HDFS découpe les fichiers dans des blocs d'une taille de 128Mo et garantit que les données stockées dans un même bloc seront sur une même machine. On souhaite donc découper le ciel en zones suffisamment petites pour tenir dans un bloc.

En première approximation, pour mettre en place la structure de partitionnement, on considérera que la voûte céleste est représentée par un rectangle avec des coordonnées cartésiennes (ce qui est complètement faux, les coordonnées fournies étant des [coordonnées célestes](https://fr.wikipedia.org/wiki/Coordonn%C3%A9es_sph%C3%A9riques#Coordonn.C3.A9es_c.C3.A9lestes), déclinaison(`decl`) et ascension droite (`ra`)).

Pour réaliser le partitionnement, il est utile de connaître les informations suivantes:

* Les limites de la zone du ciel correspondant au jeu de données (i.e. les valeurs extrémales des attributs `ra`et `decl`)
* Le nombre minimum de blocs à considérer

Pour calculer les premières informations, on peut par exemple s'appuyer sur une classe `Zone`:

```scala
case class Zone(minRA: Double, maxRA: Double,
                minDecl: Double, maxDecl: Double) {
    def zone_englobante(b: Zone): Zone = {
       // à compléter
       // calcule une zone qui englobe this et b
    }
} 
```

On peut ensuite découper le ciel en une grille (1 bloc HDFS correspondant idéalement à 1 case de la grille) et pouvoir, étant données ses coordonnées, déterminer dans quelle case une observation est à placer.
Il est conseillé de créer une classe `Grille` à cet effet.
Le nombre minimum de blocs sera utilisé pour déterminer la "finesse" de la grille.

Remarque: l'index de la colonne `ra` (resp. `decl`) est donné par `PetaSkySchema.s_ra` (resp. `PetaSkySchema.s_decl`).
 
## Programme de partitionnement

Écrire une ou plusieurs applications Spark qui réalisent le partitionnement d'un ensemble de sources (fournies sous la forme de fichiers CSV comme pour le TP précédent) et qui écrivent les données partitionnées dans le HDFS à raison d'un répertoire par case de la grille.
Idéalement, ce programme affichera ou enregistrera en plus un fichier contenant les informations de partitionnement (par exemple dimensions de la grille, coordonnées des cases, etc).

Il est conseillé de commencer localement avec le fichier _sample_ fourni en exemple. 
On pourra ensuite travailler sur un fichier source du répertoire HDFS `/tp-data/Source` (~80Mo de données). 
Enfin on fera s'exécuter le partitionnement sur l'ensemble du répertoire `Source` (~5Go de données).

Afin de simplifier l'écriture d'un RDD dans des fichiers en fonction d'une valeur, on pourra utiliser la fonction `TIW6RDDUtils.writePairRDDToHadoopUsingKeyAsFileName` (fournie dans le fichier `TIW6RDDUtils.scala`).

Calculer (et intégrer dans [COMPTE_RENDU.md](COMPTE_RENDU.md)) l'histogramme qui donne le nombre de lignes stockées dans chaque fichier résultant du partitionnement.
Commenter le résultat dans [COMPTE_RENDU.md](COMPTE_RENDU.md).

Il est demandé de ne **pas** recopier les fichiers de `tp-data/Source` dans votre répertoire utilisateur. 
Seuls les fichiers (_i.e._ les données partitionnées) produits par le programme sont à placer dans votre répertoire HDFS `/user/pxxxxx`.
Penser également à supprimer les fichiers inutiles au fur et à mesure (et à vider la corbeille).

## Extensions

Comme souligné plus haut, la première version du programme reste très approximative, il faudrait donc améliorer ce partitionnement. On peut par exemple:
* En remarquant que les observations au bord d'une case peuvent être considérées comme proches des observations d'une autre case, il faudrait dupliquer ces observations dans le bloc de cette autre case.
* Certaines zones du ciel peuvent être plus chargées que d'autres et la géométrie de la grille peut être imparfaite. On peut donc être intéressé à rediviser certaines cases trop chargées. Cela complexifie la structure de la grille et il peut être utile de stocker cette structure dans un fichier.
* Prendre en compte correctement la géométrie des coordonnées célestes.


