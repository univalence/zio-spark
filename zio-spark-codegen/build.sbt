ThisBuild / organization := "io.univalence"

lazy val plugin =
  (project in file("."))
    .enablePlugins(SbtPlugin)
    .settings(
      name := "zio-spark-codegen",
      libraryDependencies ++= Seq(
        "dev.zio"       %% "zio"              % "2.0.0-RC2",
        "dev.zio"       %% "zio-test"         % "2.0.0-RC2" % Test,
        "dev.zio"       %% "zio-test-sbt"     % "2.0.0-RC2" % Test,
        "org.scalameta" %% "scalafmt-dynamic" % "3.3.0", // equals to sbt-scalafmt's scalfmt-dynamic version
        "org.scalameta" %% "scalameta"        % "4.4.35"
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
