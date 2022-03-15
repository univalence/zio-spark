name         := "using-older-spark-version"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "io.univalence"    %% "zio-spark"  % "0.1.0",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql"  % "2.4.0"
)
