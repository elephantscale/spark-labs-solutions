name := "over-TCP"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.3.0"  % "provided"
)

