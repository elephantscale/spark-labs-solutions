name := "Structured Streaming"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
"org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.4.5"  % "provided",
"com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
"com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"
)


// ignore files in .ipynb_checkpoints
excludeFilter in (Compile, unmanagedSources) ~= { _ ||
  new FileFilter {
    def accept(f: File) = f.getPath.containsSlice("/.ipynb_checkpoints/")
  } }
