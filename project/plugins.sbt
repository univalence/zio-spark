addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.10.4")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"   % "1.5.10")
addSbtPlugin("io.github.davidgregory084"         % "sbt-tpolecat"     % "0.4.1")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.4.6")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"    % "2.0.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")

lazy val codegen =
  project
    .in(file("."))
    .dependsOn(ProjectRef(file("../zio-spark-codegen"), "plugin"))
