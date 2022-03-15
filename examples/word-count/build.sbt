name         := "word-count"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "io.univalence"    %% "zio-spark"  % "0.1.0",
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql"  % "3.2.1"
)
