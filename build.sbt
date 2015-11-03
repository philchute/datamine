
name := "scala-clustering-app"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-mllib" % "1.4.1",
  "net.liftweb" %% "lift-json" % "2.5",
  "net.liftweb" %% "lift-webkit" % "2.5"
)