// Common configuration
inThisBuild(
  List(
    version ~= addVersionPadding,
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
    "zio\\.spark\\.sql\\.KeyValueGroupedDataset.*", // Generated source
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
lazy val zio        = "2.0.5"
lazy val zioPrelude = "1.0.0-RC16"

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.17"
lazy val scala213 = "2.13.8"
lazy val scala31  = "3.2.1"

lazy val supportedScalaVersions = List(scala211, scala212, scala213, scala31)

lazy val scalaMajorVersion: SettingKey[Long] = SettingKey("scala major version")
lazy val scalaMinorVersion: SettingKey[Long] = SettingKey("scala minor version")

lazy val core =
  (project in file("zio-spark-core"))
    .configs(IntegrationTest)
    .settings(crossScalaVersionSettings)
    .settings(
      name := "zio-spark",
      resolvers ++= Resolver.sonatypeOssRepos("snapshots"),
      crossScalaVersions := supportedScalaVersions,
      scalaVersion       := scala213,
      scalaMajorVersion  := CrossVersion.partialVersion(scalaVersion.value).get._1,
      scalaMinorVersion  := CrossVersion.partialVersion(scalaVersion.value).get._2,
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio-test"     % zio % Test,
        "dev.zio" %% "zio-test-sbt" % zio % Test,
        "dev.zio" %% "zio"          % zio,
        "dev.zio" %% "zio-streams"  % zio,
        "dev.zio" %% "zio-prelude"  % zioPrelude
      ) ++ generateSparkLibraryDependencies(scalaMajorVersion.value, scalaMinorVersion.value)
        ++ generateMagnoliaDependency(scalaMajorVersion.value, scalaMinorVersion.value),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
      scalacOptions ~= fatalWarningsAsProperties,
      Defaults.itSettings
    )
    .enablePlugins(ZioSparkCodegenPlugin)

def example(project: Project): Project =
  project
    .dependsOn(core)
    // run is forcing the exit of sbt. It could be useful to set fork to true
    /* however, the base directory of the fork is set to the subproject root (./examples/simple-app) instead of the
     * project root (./) */
    /* which lead to errors, eg. Path does not exist:
     * file:./zio-spark/examples/simple-app/examples/simple-app/src/main/resources/data.csv */
    .settings(
      fork           := false,
      publish / skip := true
    )

lazy val exampleSimpleApp              = (project in file("examples/simple-app")).configure(example)
lazy val exampleSparkCodeMigration     = (project in file("examples/spark-code-migration")).configure(example)
lazy val exampleUsingOlderSparkVersion = (project in file("examples/using-older-spark-version")).configure(example)
lazy val exampleWordCount              = (project in file("examples/word-count")).configure(example)
lazy val exampleZIOEcosystem =
  (project in file("examples/zio-ecosystem"))
    .configure(example)
    .dependsOn(
      exampleSimpleApp,
      exampleSparkCodeMigration,
      exampleWordCount
    )

lazy val examples =
  (project in file("examples")).aggregate(
    exampleSimpleApp,
    exampleSparkCodeMigration,
    exampleUsingOlderSparkVersion,
    exampleWordCount,
    exampleZIOEcosystem
  )

/** Generates required libraries for magnolia. */
def generateMagnoliaDependency(scalaMajor: Long, scalaMinor: Long): Seq[ModuleID] =
  scalaMinor match {
    case _ if scalaMajor == 3 => Seq("com.softwaremill.magnolia1_3" %% "magnolia" % "1.2.0")
    case 11                   => Seq("me.lyh" %% "magnolia" % "0.12.1.0-b575bf3")
    case 12 | 13              => Seq("com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.2")
    case _                    => throw new Exception("It should be unreachable.")
  }

/** Generates required libraries for spark. */
def generateSparkLibraryDependencies(scalaMajor: Long, scalaMinor: Long): Seq[ModuleID] =
  scalaMajor match {
    case 2 =>
      val sparkVersion: String = sparkScalaVersionMapping(scalaMinor)

      Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion % Provided withSources (),
        "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided withSources ()
      )
    case 3 =>
      val sparkVersion: String = sparkScalaVersionMapping(13)

      Seq(
        ("org.apache.spark" %% "spark-core" % sparkVersion % Provided withSources ()).cross(CrossVersion.for3Use2_13),
        ("org.apache.spark" %% "spark-sql"  % sparkVersion % Provided withSources ()).cross(CrossVersion.for3Use2_13),
        "io.github.vincenzobaz" %% "spark-scala3" % "0.1.3"
      )
    case _ => throw new Exception("It should be unreachable.")
  }

/**
 * Returns the correct spark version depending of the current scala
 * minor.
 */
def sparkScalaVersionMapping(scalaMinor: Long): String =
  scalaMinor match {
    case 11 => "2.4.8"
    case 12 => "3.2.0"
    case 13 => "3.2.0"
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

/**
 * Add padding to change: 0.1.0+48-bfcea99ap20220317-1157-SNAPSHOT into
 * 0.1.0+0048-bfcea99ap20220317-1157-SNAPSHOT. It helps to retrieve the
 * latest snapshots from
 * https://oss.sonatype.org/#nexus-search;gav~io.univalence~zio-spark_2.13~~~~kw,versionexpand.
 */
def addVersionPadding(baseVersion: String): String = {
  import scala.util.matching.Regex

  val paddingSize    = 5
  val counter: Regex = "\\+([0-9]+)-".r

  counter.findFirstMatchIn(baseVersion) match {
    case Some(regex) =>
      val count          = regex.group(1)
      val snapshotNumber = "0" * (paddingSize - count.length) + count
      counter.replaceFirstIn(baseVersion, s"+$snapshotNumber-")
    case None => baseVersion
  }
}

def scalaVersionSpecificSources(environment: String, baseDirectory: File)(versions: String*) =
  for {
    version <- "scala" :: versions.toList.map("scala-" + _)
    result = baseDirectory / "src" / environment / version
    if result.exists
  } yield result

def crossScalaVersionSources(scalaVersion: String, environment: String, baseDir: File) = {
  val versions =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 11)) =>
        List("2", "2.11+", "2.11-2.12")
      case Some((2, 12)) =>
        List("2", "2.11+", "2.12+", "2.11-2.12", "2.12-2.13")
      case Some((2, 13)) =>
        List("2", "2.11+", "2.12+", "2.13+", "2.12-2.13")
      case Some((3, _)) =>
        List("2.11+", "2.12+", "2.13+")
      case _ =>
        List()
    }
  scalaVersionSpecificSources(environment, baseDir)(versions: _*)
}

lazy val crossScalaVersionSettings =
  Seq(
    Compile / unmanagedSourceDirectories ++= {
      crossScalaVersionSources(
        scalaVersion.value,
        "main",
        baseDirectory.value
      )
    },
    Test / unmanagedSourceDirectories ++= {
      crossScalaVersionSources(
        scalaVersion.value,
        "test",
        baseDirectory.value
      )
    }
  )
