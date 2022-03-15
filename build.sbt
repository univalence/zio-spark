// Common configuration
inThisBuild(
  List(
    organization  := "io.univalence",
    homepage      := Some(url("https://github.com/univalence/zio-spark")),
    licenses      := List("Apache-2.0" -> url("https://github.com/univalence/zio-spark/blob/master/LICENSE")),
    versionScheme := Some("early-semver"),
    developers := List(
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
  )
)

// Scalafix configuration
ThisBuild / scalafixScalaBinaryVersion := "2.13"
ThisBuild / semanticdbEnabled          := true
ThisBuild / semanticdbVersion          := scalafixSemanticdb.revision
ThisBuild / scalafixDependencies ++= Seq("com.github.vovapolu" %% "scaluzzi" % "0.1.21")

// SCoverage configuration
val excludedPackages: Seq[String] =
  Seq(
    "zio\\.spark\\.rdd\\.RDD.*",                    // Generated source
    "zio\\.spark\\.sql\\.Dataset.*",                // Generated source
    "zio\\.spark\\.sql\\.DataFrameNaFunctions.*",   // Generated source
    "zio\\.spark\\.sql\\.DataFrameStatFunctions.*", // Generated source
    "zio\\.spark\\.sql\\.implicits.*",              // Spark implicits
    "zio\\.spark\\.sql\\.LowPrioritySQLImplicits.*" // Spark implicits
  )

ThisBuild / coverageFailOnMinimum           := false
ThisBuild / coverageMinimumStmtTotal        := 80
ThisBuild / coverageMinimumBranchTotal      := 80
ThisBuild / coverageMinimumStmtPerPackage   := 50
ThisBuild / coverageMinimumBranchPerPackage := 50
ThisBuild / coverageMinimumStmtPerFile      := 0
ThisBuild / coverageMinimumBranchPerFile    := 0
ThisBuild / coverageExcludedPackages        := excludedPackages.mkString(";")

// Aliases
addCommandAlias("fmt", "scalafmt")
addCommandAlias("fmtCheck", "scalafmtCheckAll")
addCommandAlias("lint", "scalafix")
addCommandAlias("lintCheck", "scalafixAll --check")
addCommandAlias("check", "; fmtCheck; lintCheck;")
addCommandAlias("fixStyle", "; scalafmtAll; scalafixAll;")
addCommandAlias("prepare", "fixStyle")
addCommandAlias("testAll", "; clean;+ test;")
addCommandAlias("testSpecific", "; clean; test;")
addCommandAlias("testSpecificWithCoverage", "; clean; coverage; test; coverageReport;")

// -- Lib versions
lazy val libVersion =
  new {
    val zio        = "2.0.0-RC2"
    val zioPrelude = "1.0.0-RC10"
  }

lazy val scala =
  new {
    val v211 = "2.11.12"
    val v212 = "2.12.15"
    val v213 = "2.13.8"
  }

lazy val supportedScalaVersions = List(scala.v211, scala.v212, scala.v213)

lazy val scalaMajorVersion: SettingKey[Long] = SettingKey("scala major version")

lazy val core =
  (project in file("zio-spark-core"))
    .configs(IntegrationTest)
    .settings(
      name               := "zio-spark",
      crossScalaVersions := supportedScalaVersions,
      scalaVersion       := scala.v213,
      scalaMajorVersion  := CrossVersion.partialVersion(scalaVersion.value).get._2,
      libraryDependencies ++= generateLibraryDependencies(scalaMajorVersion.value),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
      scalacOptions ~= fatalWarningsAsProperties,
      Defaults.itSettings
    )
    .enablePlugins(ZioSparkCodegenPlugin)

val exampleNames =
  Seq(
    "simple-app",
    "spark-code-migration",
    "using-older-spark-version",
    "word-count"
  )

lazy val exampleSimpleApp              = (project in file("examples/simple-app")).dependsOn(core)
lazy val exampleSparkCodeMigration     = (project in file("examples/spark-code-migration")).dependsOn(core)
lazy val exampleUsingOlderSparkVersion = (project in file("examples/using-older-spark-version")).dependsOn(core)
lazy val exampleWordCount              = (project in file("examples/word-count")).dependsOn(core)

/** Generates required libraries for spark. */
def generateSparkLibraryDependencies(scalaMinor: Long): Seq[ModuleID] = {
  val sparkVersion: String = sparkScalaVersionMapping(scalaMinor)

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided
  )
}

/** Generates required libraries for a particular project. */
def generateLibraryDependencies(scalaMinor: Long): Seq[ModuleID] = {
  val sparkLibraries = generateSparkLibraryDependencies(scalaMinor)

  sparkLibraries ++ Seq(
    "dev.zio" %% "zio-test"     % libVersion.zio % Test,
    "dev.zio" %% "zio-test-sbt" % libVersion.zio % Test,
    "dev.zio" %% "zio"          % libVersion.zio,
    "dev.zio" %% "zio-streams"  % libVersion.zio,
    "dev.zio" %% "zio-prelude"  % libVersion.zioPrelude
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

/**
 * Don't fail the compilation for warnings by default, you can still
 * activate it using system properties (It should always be activated in
 * the CI).
 */
def fatalWarningsAsProperties(options: Seq[String]): Seq[String] =
  if (sys.props.getOrElse("fatal-warnings", "false") == "true") options
  else options.filterNot(Set("-Xfatal-warnings"))
