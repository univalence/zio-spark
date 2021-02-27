import sbt.Keys._
import sbt._
import sbtdynver.GitCommitSuffix

val libVersion =
  new {
    val zio              = "1.0.4-2"
    val scala2_12        = "2.12.13"
    val organize_imports = "0.5.0"
    val scaluzzi         = "0.1.16"
  }

scmInfo := Some(
  ScmInfo(
    url("https://github.com/univalence/zio-spark"),
    "scm:git:https://github.com/univalence/zio-spark.git",
    "scm:git:git@github.com:univalence/zio-spark.git"
  )
)

developers := List(
  Developer(
    id = "jwinandy",
    name = "Jonathan Winandy",
    email = "jonathan@univalence.io",
    url = url("https://github.com/ahoy-jon")
  ),
  Developer(
    id = "phong",
    name = "Philippe Hong",
    email = "philippe@univalence.io",
    url = url("https://github.com/hwki77")
  ),
  Developer(
    id = "fsarradin",
    name = "François Sarradin",
    email = "francois@univalence.io",
    url = url("https://github.com/fsarradin")
  ),
  Developer(
    id = "bernit77",
    name = "Bernarith Men",
    email = "bernarith@univalence.io",
    url = url("https://github.com/bernit77")
  ),
  Developer(
    id = "HarrisonCheng",
    name = "Harrison Cheng",
    email = "harrison@univalence.io",
    url = url("https://github.com/HarrisonCheng")
  ),
  Developer(
    id = "dylandoamaral",
    name = "Dylan Do Amaral",
    email = "dylan@univalence.io",
    url = url("https://github.com/dylandoamaral")
  )
)

organization := "io.univalence"
organizationName := "Univalence"
organizationHomepage := Some(url("https://univalence.io/"))
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
name := "zio-spark"
description := "Spark Zio is to use Zio with Spark"
homepage := Option(url("https://github.com/univalence/zio-spark"))

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"          % libVersion.zio,
  "dev.zio" %% "zio-streams"  % libVersion.zio,
  "dev.zio" %% "zio-test"     % libVersion.zio % "test",
  "dev.zio" %% "zio-test-sbt" % libVersion.zio % "test"
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))

scalafixDependencies in ThisBuild += "com.github.vovapolu" %% "scaluzzi" % "0.1.16"

scalaVersion := libVersion.scala2_12

scalacOptions --= Seq(
  "-Xcheckinit",     // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings" // Fail the compilation if there are any warnings.
)
//scalacOptions += "-Yrangepos"
//scalacOptions += "-Ywarn-unused"

scalafmtOnCompile := false

publishTo := sonatypePublishTo.value
releaseEarlyEnableSyncToMaven := false
releaseEarlyWith in Global := SonatypePublisher

bintrayOrganization := Some("univalence")
bintrayRepository := "univalence-jvm"
bintrayPackageLabels := Seq("spark", "scala")
releaseEarlyEnableSyncToMaven := false
releaseEarlyWith in Global := BintrayPublisher

useSpark("sql", "core")

def useSpark(modules: String*) =
  libraryDependencies ++= modules.map(name => "org.apache.spark" %% s"spark-$name" % "3.1.0" % Provided)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")

addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/"
)

onLoadMessage := {
  def header(text: String): String =
    s"${scala.Console.GREEN}$text${scala.Console.RESET}"

  s"""|${header(raw"  ___________ ____     _____                  _     ")}
      |${header(raw" |___  /_   _/ __ \   / ____|                | |    ")}
      |${header(raw"    / /  | || |  | | | (___  _ __   __ _ _ __| | __ ")}
      |${header(raw"   / /   | || |  | |  \___ \| '_ \ / _` | '__| |/ / ")}
      |${header(raw"  / /__ _| || |__| |  ____) | |_) | (_| | |  |   <  ")}
      |${header(raw" /_____|_____\____/  |_____/| .__/ \__,_|_|  |_|\_\ ")}
      |${header(raw"                            | |                     ")}
      |${header(raw"                            |_|                     ")}
      |${header(s"version: ${Keys.version.value}")}""".stripMargin
}

def versionFmt(out: sbtdynver.GitDescribeOutput): String = {
  def mkString(suffix: GitCommitSuffix): String =
    if (suffix.isEmpty) "" else f"-${suffix.distance}%05d-${suffix.sha}"
  (out.ref.dropV.value
    + mkString(out.commitSuffix)
    + out.dirtySuffix.dropPlus.mkString("-", ""))
}

def fallbackVersion(d: java.util.Date): String = s"HEAD-${sbtdynver.DynVer timestamp d}"

inThisBuild(
  List(
    version := dynverGitDescribeOutput.value.mkVersion(versionFmt, fallbackVersion(dynverCurrentDate.value)),
    dynver := {
      val d = new java.util.Date
      sbtdynver.DynVer.getGitDescribeOutput(d).mkVersion(versionFmt, fallbackVersion(d))
    },
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalafixDependencies ++= Seq(
      "com.github.liancheng" %% "organize-imports" % libVersion.organize_imports,
      "com.github.vovapolu"  %% "scaluzzi"         % libVersion.scaluzzi
    )
  )
)
