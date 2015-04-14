name := "playground"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.1.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2"

addCommandAlias("make-idea", ";update-classifiers; update-sbt-classifiers; gen-idea sbt-classifiers")

addCommandAlias("generate-project",
  ";update-classifiers;update-sbt-classifiers;gen-idea sbt-classifiers")
