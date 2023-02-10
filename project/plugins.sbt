ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.10.4")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"   % "1.5.11")
addSbtPlugin("io.github.davidgregory084"         % "sbt-tpolecat"     % "0.4.2")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.5.0")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"    % "2.0.7")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")

lazy val codegen =
  project
    .in(file("."))
    .dependsOn(ProjectRef(file("../zio-spark-codegen"), "plugin"))
