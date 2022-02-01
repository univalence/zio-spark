// Common configuration
ThisBuild / organization         := "io.univalence"
ThisBuild / organizationName     := "Univalence"
ThisBuild / organizationHomepage := Some(url("https://univalence.io/"))

ThisBuild / version                  := "0.1.0"
ThisBuild / description              := "Imagine if ZIO and Spark made a baby !"
ThisBuild / startYear                := Some(2022)
ThisBuild / licenses += ("Apache-2.0" → new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage                 := Some(url("https://github.com/univalence/zio-spark"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/univalence/zio-spark"),
    "scm:git:https://github.com/univalence/zio-spark.git",
    "scm:git:git@github.com:univalence/zio-spark.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "jwinandy",
    name  = "Jonathan Winandy",
    email = "jonathan@univalence.io",
    url   = url("https://github.com/ahoy-jon")
  ),
  Developer(
    id    = "phong",
    name  = "Philippe Hong",
    email = "philippe@univalence.io",
    url   = url("https://github.com/hwki77")
  ),
  Developer(
    id    = "fsarradin",
    name  = "François Sarradin",
    email = "francois@univalence.io",
    url   = url("https://github.com/fsarradin")
  ),
  Developer(
    id    = "bernit77",
    name  = "Bernarith Men",
    email = "bernarith@univalence.io",
    url   = url("https://github.com/bernit77")
  ),
  Developer(
    id    = "HarrisonCheng",
    name  = "Harrison Cheng",
    email = "harrison@univalence.io",
    url   = url("https://github.com/HarrisonCheng")
  ),
  Developer(
    id    = "dylandoamaral",
    name  = "Dylan Do Amaral",
    email = "dylan@univalence.io",
    url   = url("https://github.com/dylandoamaral")
  )
)

// Scalafix configuration
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixDependencies ++= Seq(
  "com.github.vovapolu" %% "scaluzzi" % "0.1.21"
)

// SCoverage configuration
ThisBuild / coverageFailOnMinimum           := true
ThisBuild / coverageMinimumStmtTotal        := 80
ThisBuild / coverageMinimumBranchTotal      := 80
ThisBuild / coverageMinimumStmtPerPackage   := 80
ThisBuild / coverageMinimumBranchPerPackage := 80
ThisBuild / coverageMinimumStmtPerFile      := 50
ThisBuild / coverageMinimumBranchPerFile    := 50

addCommandAlias("fmt", "scalafmt")
addCommandAlias("fmtCheck", "scalafmtCheck")
addCommandAlias("lint", "scalafix")
addCommandAlias("lintCheck", "scalafix --check")
addCommandAlias("fixStyle", "; scalafmt; scalafix;")
addCommandAlias("testSpecific", "; clean; test;")
addCommandAlias("testAll", "; clean;+ test;")
addCommandAlias("testWithCoverage", "; clean; coverage;+ test; coverageReport;")

// -- Lib versions
lazy val libVersion =
  new {
    val zio1 = "1.0.13"
    val zio2 = "2.0.0-RC2"
  }

lazy val scala =
  new {
    val v211 = "2.11.12"
    val v212 = "2.12.15"
    val v213 = "2.13.8"
  }

lazy val supportedScalaVersions = List(scala.v211, scala.v212, scala.v213)

lazy val newZioSpark =
  (project in file("new"))
    .settings(
      name               := "zio-spark-new",
      crossScalaVersions := supportedScalaVersions,
      scalaVersion       := scala.v213,
      libraryDependencies ++= generateLibraryDependencies(
        libVersion.zio2,
        CrossVersion.partialVersion(scalaVersion.value).get._2
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
lazy val zioSpark =
  (project in file("."))
    .settings(
      name         := "zio-spark",
      scalaVersion := scala.v212,
      libraryDependencies ++= generateLibraryDependencies(
        libVersion.zio1,
        CrossVersion.partialVersion(scalaVersion.value).get._2
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

/** Generates required libraries for a particular project. */
def generateLibraryDependencies(zioVersion: String, scalaMinor: Long): Seq[ModuleID] = {
  val sparkVersion = sparkScalaVersionMapping(scalaMinor)

  Seq(
    "org.apache.spark" %% "spark-core"   % sparkVersion,
    "org.apache.spark" %% "spark-sql"    % sparkVersion % "provided",
    "dev.zio"          %% "zio-test"     % zioVersion   % Test,
    "dev.zio"          %% "zio-test-sbt" % zioVersion   % Test,
    "dev.zio"          %% "zio"          % zioVersion
  )
}

/**
 * Returns the correct spark version depending of the current scala
 * minor.
 */
def sparkScalaVersionMapping(scalaMinor: Long): String =
  scalaMinor match {
    case 11 => "2.1.3"
    case 12 => "2.4.8"
    case 13 => "3.2.1"
    case _  => throw new Exception("It should be unreachable.")
  }
