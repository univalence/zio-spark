ThisBuild / organization := "io.univalence"

def sparkModule(name: String) = "org.apache.spark" %% s"spark-$name" % "3.2.1" withSources ()

lazy val plugin =
  (project in file("."))
    .enablePlugins(SbtPlugin)
    .settings(
      name := "zio-spark-codegen",
      libraryDependencies ++= Seq(
        "dev.zio"       %% "zio"          % "2.0.0-RC2",
        "dev.zio"       %% "zio-test"     % "2.0.0-RC2" % Test,
        "dev.zio"       %% "zio-test-sbt" % "2.0.0-RC2" % Test,
        "org.scalameta" %% "scalameta"    % "4.4.35",
        sparkModule("core"),
        sparkModule("sql")
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
