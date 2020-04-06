ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "flink-scalacheck-generator"

version := "1.3.7"

organization := "org.example"

val flinkVersion = "1.10.0"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table-planner" % flinkVersion)


// Libraries for ScalaCheck
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.1"
//libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "4.8.3",
  // the scalacheck lib will come as a transitive
  // dependency
  "org.specs2" %% "specs2-scalacheck" % "4.8.3"
)

scalacOptions in Test ++= Seq("-Yrangepos")