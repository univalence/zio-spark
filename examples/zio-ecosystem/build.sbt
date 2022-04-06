name         := "zio-ecosystem"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  // "io.univalence"    %% "zio-spark"  % "X.X.X", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core"  % "3.2.1",
  "org.apache.spark" %% "spark-sql"   % "3.2.1",
  "dev.zio"          %% "zio-cli"     % "0.2.4-1",
  "dev.zio"          %% "zio-logging" % "2.0.0-RC7"
)
