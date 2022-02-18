ThisBuild / organization := "io.univalence"

lazy val plugin =
  (project in file("."))
    .enablePlugins(SbtPlugin)
    .settings(
      name := "zio-spark-codegen",
      libraryDependencies ++= Seq(
        "dev.zio"       %% "zio"       % "2.0.0-RC2",
        "org.scalameta" %% "scalameta" % "4.4.35"
      )
    )
