name         := "using-older-spark-version"
scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  // "io.univalence"    %% "zio-spark"  % "X.X.X", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.apache.spark" %% "spark-sql"  % "3.2.4"
)
