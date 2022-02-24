ThisBuild / organization := "io.univalence"

def sparkModule(name: String) = "org.apache.spark" %% s"spark-$name" % "3.2.1" % Provided

lazy val plugin =
  (project in file("."))
    .enablePlugins(SbtPlugin)
    .settings(
      name := "zio-spark-codegen",
      libraryDependencies ++= Seq(
        "dev.zio"          %% "zio"          % "2.0.0-RC2",
        "org.scalameta"    %% "scalameta"    % "4.4.35",
        "org.apache.spark" %% "spark-sql"    % "3.2.1",
        "dev.zio"          %% "zio-test"     % "2.0.0-RC2" % Test,
        "dev.zio"          %% "zio-test-sbt" % "2.0.0-RC2" % Test,
        "org.apache.spark" %% "spark-core"   % "3.2.1" withSources ()
        //
        /* /Users/dylandoamaral/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-core_2.13/3.2.1 */
        // spark-core_2.13-3.2.1-sources.jar
        // TODO : depends on org/apache/spark/spark-core_2.13/3.2.1/spark-core_2.13-3.2.1-sources.jar
        // TODO : depends on org/apache/spark/spark-core_2.13/3.2.1/spark-sql_2.13-3.2.1-sources.jar
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
