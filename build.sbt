// Scala configuration
ThisBuild / crossScalaVersions := Seq("2.13.8")
ThisBuild / scalaVersion       := crossScalaVersions.value.head

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

// -- Lib versions
lazy val libVersion =
  new {
    // -- Test
    val scalatest = "3.2.10"

    // -- ZIO
    val zio = "2.0.0-RC1"

    // -- Spark
    val spark = "3.2.0"
  }

// -- Main project settings
lazy val root =
  (project in file("."))
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core"   % libVersion.spark,
        "org.apache.spark" %% "spark-sql"    % libVersion.spark     % "provided",
        "dev.zio"          %% "zio-test"     % libVersion.zio       % Test,
        "dev.zio"          %% "zio-test-sbt" % libVersion.zio       % Test,
        "dev.zio"          %% "zio"          % libVersion.zio,
        "org.scalatest"    %% "scalatest"    % libVersion.scalatest % Test
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

lazy val metadataSettings =
  Def.settings(
    // -- Organization
    organization         := "io.univalence",
    organizationName     := "Univalence",
    organizationHomepage := Some(url("http://univalence.io/")),
    // -- Project
    name                     := "zio-spark",
    version                  := "0.1.0",
    description              := "Imagine if ZIO and Spark made a baby",
    startYear                := Some(2022),
    licenses += ("Apache-2.0" → new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage                 := Some(url("https://github.com/univalence/zio-spark")), // TODO
    // -- Contributors
    developers := List(), // TODO
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/univalence/zio-spark"),
        "scm:git:https://github.com/univalence/zio-spark.git",
        "scm:git:git@github.com:univalence/zio-spark.git"
      )
    )
  )

lazy val scalaSettings =
  Def.settings(
    crossScalaVersions := Seq("2.13.8"),
    scalaVersion       := crossScalaVersions.value.head
  )

// Coverage configuration
coverageFailOnMinimum           := true
coverageMinimumStmtTotal        := 80
coverageMinimumBranchTotal      := 80
coverageMinimumStmtPerPackage   := 80
coverageMinimumBranchPerPackage := 80
coverageMinimumStmtPerFile      := 50
coverageMinimumBranchPerFile    := 50
