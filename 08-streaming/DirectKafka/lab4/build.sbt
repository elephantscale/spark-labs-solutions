//import AssemblyKeys._


name := "es-X"

version := "0.1"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1"
)

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" exclude("com.google.guava", "guava")

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.2.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha3"

//assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
