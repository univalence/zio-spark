lazy val scala =
  new {
    val v211 = "2.11.12"
    val v212 = "2.12.15"
    val v213 = "2.13.8"
  }

lazy val supportedScalaVersions = List(scala.v211, scala.v212, scala.v213)

ThisBuild / organization       := "io.univalence"
ThisBuild / sbtPlugin          := true
ThisBuild / scalaVersion       := scala.v213
ThisBuild / crossScalaVersions := supportedScalaVersions
ThisBuild / libraryDependencies ++= Seq(
  // "dev.zio" %% "zio" % "2.0.0-RC2"
  // "org.scalameta" %% "scalameta" % "4.4.35"
)
