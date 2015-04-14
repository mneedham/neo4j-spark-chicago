name := "playground"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.3.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.5.2"

ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"

addCommandAlias("make-idea", ";update-classifiers; update-sbt-classifiers; gen-idea sbt-classifiers")

addCommandAlias("generate-project",
  ";update-classifiers;update-sbt-classifiers;gen-idea sbt-classifiers")
