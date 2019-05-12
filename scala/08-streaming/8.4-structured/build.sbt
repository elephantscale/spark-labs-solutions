name := "Structured Streaming"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
"org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.1.0"  % "provided",
"com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3",
"com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"
)

