ThisBuild / organization := "io.univalence"

// Aliases
addCommandAlias("fmt", "scalafmt")
addCommandAlias("fmtCheck", "scalafmtCheckAll")
addCommandAlias("check", "; fmtCheck;")

lazy val plugin =
  (project in file("."))
    .enablePlugins(SbtPlugin)
    .settings(
      name := "zio-spark-codegen",
      libraryDependencies ++= Seq(
        "dev.zio"          %% "zio"              % "2.0.4",
        "dev.zio"          %% "zio-test"         % "2.0.4" % Test,
        "dev.zio"          %% "zio-test-sbt"     % "2.0.4" % Test,
        "org.scalameta"    %% "scalafmt-dynamic" % "3.4.3", // equals to sbt-scalafmt's scalfmt-dynamic version
        "org.scalameta"    %% "scalameta"        % "4.7.0",
        "org.apache.spark" %% "spark-core"       % "3.3.1" withSources (), // For tests only
        "org.apache.spark" %% "spark-sql"        % "3.3.1" withSources () // For tests only
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
