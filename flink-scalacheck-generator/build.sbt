ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "flink-scalacheck-generator"

version := "1.3.8"

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

//Specs 2
libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "4.8.3",
  // the scalacheck lib will come as a transitive
  // dependency
  "org.specs2" %% "specs2-scalacheck" % "4.8.3"
)

scalacOptions in Test ++= Seq("-Yrangepos")

//ScalaMeter
//scalaSource in Compile := baseDirectory.value / "src"
//scalaSource in Test := baseDirectory.value / "test"

libraryDependencies ++= Seq(
  "com.storm-enroute" %% "scalameter" % "0.8.2" // ScalaMeter version is set in version.sbt
)
resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
fork in Test in ThisBuild := true //This line is needed to run Scalameter OffLineReport due to it uses a different JVM to run the test


//Utils
parallelExecution in Test := false
connectInput in Test := true //To accept user input




