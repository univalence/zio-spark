name         := "using-older-spark-version"
scalaVersion := "2.13.8"

crossScalaVersions := Seq("2.12.15", "2.13.8")

def sparkVersion(scalaVersion: Long): String =
  scalaVersion match {
    case 13 => "3.2.1"
    case 12 => "2.4.8" // USING A OLD VERSION FOR 2.12 ON PURPOSE
  }

libraryDependencies ++= {
  val minorVersion = CrossVersion.partialVersion(scalaVersion.value).get._2
  val version      = sparkVersion(minorVersion)
  Seq(
    // "io.univalence"    %% "zio-spark"  % "X.X.X", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
    "org.apache.spark" %% "spark-core" % version,
    "org.apache.spark" %% "spark-sql"  % version
  )

}
