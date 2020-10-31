// blank lines are important!

name := "TestApp"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"
)

// for accessing files from S3 or HDFS
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" exclude("com.google.guava", "guava")

// https://mvnrepository.com/artifact/org.specs2/specs2-core
libraryDependencies += "org.specs2" %% "specs2-core" % "4.5.1" % Test

// ignore files in .ipynb_checkpoints
excludeFilter in (Compile, unmanagedSources) ~= { _ ||
  new FileFilter {
    def accept(f: File) = f.getPath.containsSlice("/.ipynb_checkpoints/")
  } }



