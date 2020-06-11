import sbt.Keys._
import sbt._

val libVersion =
  new {
    val zio       = "1.0.0-RC20"
    val scala2_12 = "2.12.11"
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
  "dev.zio" %% "zio-test-sbt" % libVersion.zio % "test",
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))

scalafixDependencies in ThisBuild += "com.github.vovapolu" %% "scaluzzi" % "0.1.2"

scalaVersion := libVersion.scala2_12

scalacOptions := stdOptions
scalacOptions += "-Yrangepos"
scalacOptions += "-Ywarn-unused"

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
  libraryDependencies ++= modules.map(name => "org.apache.spark" %% s"spark-$name" % "2.4.1" % Provided)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")

addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/"
)

def stdOptions =
  (Opts.compile.encoding("utf-8")
    ++ Seq(
      Opts.compile.deprecation, // Emit warning and location for usages of deprecated APIs.
      Opts.compile.explaintypes, // Explain type errors in more detail.
      Opts.compile.unchecked, // Enable additional warnings where generated code depends on assumptions.
      "-feature", // Emit warning and location for usages of features that should be imported explicitly.
      "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
      "-language:experimental.macros", // Allow macro definition (besides implementation and application)
      "-language:higherKinds",         // Allow higher-kinded types
      "-language:implicitConversions", // Allow definition of implicit functions called views
      //           "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
      //            "-Xfatal-warnings", // Fail the compilation if there are any warnings.
      "-Xfuture", // Turn on future language features.
      "-Yrangepos", // Use range positions for syntax trees.
      "-Ywarn-numeric-widen", // Warn when numerics are widened.
      "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
      "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
      "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
      "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
      "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
      "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
      "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
      "-Xlint:option-implicit", // Option.apply used implicit view.
      "-Xlint:package-object-classes", // Class or object defined in package object.
      "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
      "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
      "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
      "-Xlint:unsound-match", // Pattern match may not be typesafe.
      "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
      "-Ypartial-unification", // Enable partial unification in type constructor inference
      "-Ywarn-dead-code", // Warn when dead code is identified.
      "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
      "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
      "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
      "-Ypatmat-exhaust-depth",
      "off"
    ))
