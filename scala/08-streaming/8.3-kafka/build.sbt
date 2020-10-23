name := "kafka-streaming"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"


resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"
)


javaOptions ++= Seq("-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled")

// enable this to collect all dependency jars into lib_managed
//retrieveManaged := true

// for creating a fat jar
assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}


// ignore files in .ipynb_checkpoints
excludeFilter in (Compile, unmanagedSources) ~= { _ ||
  new FileFilter {
    def accept(f: File) = f.getPath.containsSlice("/.ipynb_checkpoints/")
  } }
