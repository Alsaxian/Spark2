# Spark: aggrégation, jointure et partionnement

L'objectif de ce TP est de pousser un peu plus loin l'utilisation de Spark.
Le code fourni dans ce TP correspond au premier TP Spark, jusqu'à la troisième application Spark (Calcul de valeurs extrémales) inclue.

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
    
Le TP jusqu'au programme de partionnement initial et son application aux données complètes sera notée sur 14 points, les extensions pourront augmenter la note à raison de 3 points max par extension (20 max au total).
Il est demandé de privilégier un rendu fonctionnel (_i.e._ qui s'exécute sans erreur) à l'ajout de fonctionnalités supplémentaires.

## Problème posé

On souhaite pouvoir comparer des observations (sources) avec d'autres observations proches en termes de coordonnées (i.e. une distance `ra,decl` inférieure à une certaine valeur).
Cette opération étant initialement coûteuse (quadratique) si on s'y prend de manière naïve, on souhaite partionner les données pour optimiser les calculs postérieurs.
L'idée est de colocaliser les observations appartenant à une même région du ciel afin de limiter les recherches futures à cette région (technique dite de _blocking_).

## Blocs HDFS et grille de coordonnées

Le système HDFS découpe les fichiers dans des blocs d'une taille de 128Mo et garantit que les données stockées dans un même bloc seront sur une même machine. On souhaite donc découper le ciel en zones suffisamment petites pour tenir dans un bloc.

En première approximation, pour mettre en place la structure de partionnement, on considérera que la voûte céleste est représentée par un rectangle avec des coordonnées cartésiennes (ce qui est complètement faux, les coordonnées fournies étant des [coordonnées célestes](https://fr.wikipedia.org/wiki/Coordonn%C3%A9es_sph%C3%A9riques#Coordonn.C3.A9es_c.C3.A9lestes), déclinaison(`decl`) et ascension droite (`ra`)).

Pour réaliser le partionnement, il est utile de connaître les informations suivantes:

* Les limites de la zone du ciel correspondant au jeu de données (i.e. les valeurs extrémales des attributs `ra`et `decl`)
* Le nombre minimum de blocs à considérer

On peut ensuite découper le ciel en une grille (1 bloc HDFS correspondant à 1 case de la grille) et pouvoir étant données des coordonnées déterminer dans quelle case l'observation est à placer.

## Programme de partionnement

Écrire une ou plusieurs applications Spark qui réalise(nt) le partionnement d'un ensemble de sources (fournies sous la forme de fichiers CSV comme pour le TP précédent) et qui écrivent les données partionnées dans le HDFS à raison d'un répertoire par case de la grille.
Idéalement, ce programme affichera ou enregistrera en plus un fichier contenant les informations de partionnement (par exemple dimensions de la grille, coordonnées des cases, etc).

Il est conseillé de commencer localement avec le fichier _sample_ fourni en exemple. 
On pourra ensuite travailler sur un fichier source du répertoire HDFS `/tp-data/Source` (~80Mo de données). 
Enfin on fera s'exécuter le partionnement sur l'ensemble du répertoire `Source` (~5Go de données).

Il est demandé de ne **pas** recopier les fichiers de `tp-data/Source` dans votre répertoire utilisateur. 
Seuls les fichiers (_i.e._ les données partionnées) produits par le programme sont à placer dans votre répertoire HDFS `/user/pxxxxx`.
Penser également à supprimer les fichiers inutiles au fur et à mesure.

## Extensions

Comme souligné plus haut la première version du programme reste très approximative, il faudrait donc améliorer ce partionnement. On peut par exemple:
* Prendre en compte correctement la géométrie des coordonnées célestes.
* En remarquant que les observations au bord d'une case peuvent être considérées comme proches des observations d'une autre case, il faudrait dupliquer ces observations dans le bloc de cette autre case.
* Certaines zones du ciel euvent être plus chargées que d'autres et la géométrie de la grille peut être imparfaite. On peut donc être intéressé à rediviser certaines cases trop chargées.
* Écrire une requête qui utilise le partionnement (par exemple qui liste toutes les observations à une certaine distance d'une observation donnée).
* Il est possible de proposer votre propre amélioration.

## Liens utiles

* L'utilitaire [`screen`](https://en.wikipedia.org/wiki/GNU_Screen) permet (entre autres) de conserver un shell actif tout en se déconnectant ([quickref](http://aperiodic.net/screen/quick_reference))
* Pour sauver le contenu d'un RDD dans plusieurs fichiers, on peut suivre les indications mentionnées dans la deuxième réponse de [cette question sur Stackoverflow](https://stackoverflow.com/questions/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job). Attention, cela ne fonctionne bien que lorque la clé et la valeur sont des `String`. Il faut donc éventuellement penser à convertir avant d'écrire via `saveAsHadoopFile`.
