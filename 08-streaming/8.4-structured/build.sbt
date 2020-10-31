name := "Structured Streaming"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
"org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
"org.apache.spark" %% "spark-streaming" % "3.0.1"  % "provided",
//"com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
//"com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"
)

// ignore files in .ipynb_checkpoints
excludeFilter in (Compile, unmanagedSources) ~= { _ ||
  new FileFilter {
    def accept(f: File) = f.getPath.containsSlice("/.ipynb_checkpoints/")
  } }
