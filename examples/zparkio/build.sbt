name         := "zparkio-comparaison"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  // "io.univalence"    %% "zio-spark"  % "0.10.0", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % "3.3.1" % Provided,
  "org.apache.spark" %% "spark-sql"  % "3.3.1" % Provided
)

libraryDependencies += "dev.zio" %% "zio-logging" % "2.1.11"
