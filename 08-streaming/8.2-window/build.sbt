name := "window-count"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.4.5"  % "provided"
)

// ignore files in .ipynb_checkpoints
excludeFilter in (Compile, unmanagedSources) ~= { _ ||
  new FileFilter {
    def accept(f: File) = f.getPath.containsSlice("/.ipynb_checkpoints/")
  } }
