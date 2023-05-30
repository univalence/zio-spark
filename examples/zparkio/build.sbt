name         := "zparkio-comparaison"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  // "io.univalence"    %% "zio-spark"  % "0.10.0", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % "3.4.0" % Provided,
  "org.apache.spark" %% "spark-sql"  % "3.4.0" % Provided
)

libraryDependencies += "dev.zio" %% "zio-logging" % "2.1.12"
