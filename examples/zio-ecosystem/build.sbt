name         := "zio-ecosystem"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  // "io.univalence"    %% "zio-spark"  % "X.X.X", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql"  % "3.3.0",
  "dev.zio"          %% "zio-cli"    % "0.2.7"
)
