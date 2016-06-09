name := "CollabFilterSpark"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++=  Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.0",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3",
  "com.github.claytonm" %% "SparkMatrix" % "0.0.1"
)
    