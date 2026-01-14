name         := "zio-ecosystem"
scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  // "io.univalence"    %% "zio-spark"  % "X.X.X", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql"  % "3.3.4",
  "dev.zio"          %% "zio-cli"    % "0.7.5"
)
