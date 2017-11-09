name := "SparkTPApp2"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// Nécessaire pour les tests Spark car un seul contexte de test Spark peut être actif
parallelExecution in Test := false

