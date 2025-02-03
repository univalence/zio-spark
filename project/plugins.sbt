ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.14.0")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"   % "1.5.12")
addSbtPlugin("org.typelevel"                     % "sbt-tpolecat"     % "0.5.2")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.5.4")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"    % "2.3.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")

lazy val codegen =
  project
    .in(file("."))
    .dependsOn(ProjectRef(file("../zio-spark-codegen"), "plugin"))
