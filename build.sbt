// Scala configuration
ThisBuild / crossScalaVersions         := Seq("2.13.8")
ThisBuild / scalaVersion               := crossScalaVersions.value.head
ThisBuild / scalafixScalaBinaryVersion := "2.13"

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
    // -- ZIO
    val zio = "2.0.0-RC2"

    // -- Spark
    val spark = "3.2.0"
  }

// -- Main project settings
lazy val root =
  (project in file("new"))
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core"   % libVersion.spark,
        "org.apache.spark" %% "spark-sql"    % libVersion.spark % "provided",
        "dev.zio"          %% "zio-test"     % libVersion.zio   % Test,
        "dev.zio"          %% "zio-test-sbt" % libVersion.zio   % Test,
        "dev.zio"          %% "zio"          % libVersion.zio
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

lazy val metadataSettings =
  Def.settings(
    // -- Organization
    organization         := "io.univalence",
    organizationName     := "Univalence",
    organizationHomepage := Some(url("https://univalence.io/")),
    // -- Project
    name                     := "zio-spark",
    version                  := "0.1.0",
    description              := "Imagine if ZIO and Spark made a baby",
    startYear                := Some(2022),
    licenses += ("Apache-2.0" → new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    homepage                 := Some(url("https://github.com/univalence/zio-spark")),
    // -- Contributors
    developers := List(
      Developer(
        id    = "dylandoamaral",
        name  = "Dylan Do Amaral",
        email = "dylan@univalence.io",
        url   = url("https://github.com/dylandoamaral")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/univalence/zio-spark"),
        "scm:git:https://github.com/univalence/zio-spark.git",
        "scm:git:git@github.com:univalence/zio-spark.git"
      )
    )
  )
