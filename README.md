<h1 align="center">zio-spark</h1>

<p align="center">
  <a href="https://img.shields.io/badge/Project%20Stage-Development-yellowgreen.svg">
    <img src="https://img.shields.io/badge/Project%20Stage-Development-yellowgreen.svg" />
  </a>
  <a href="https://github.com/univalence/zio-spark/actions">
    <img src="https://github.com/univalence/zio-spark/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="https://codecov.io/gh/univalence/zio-spark">
    <img src="https://codecov.io/gh/univalence/zio-spark/branch/master/graph/badge.svg" />
  </a>
  <a href="https://scala-steward.org">
    <img src="https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=" />
  </a>
  <a href="https://index.scala-lang.org/univalence/zio-spark/zio-spark">
    <img src="https://index.scala-lang.org/univalence/zio-spark/zio-spark/latest-by-scala-version.svg?platform=jvm" />
  </a>
</p>

<p align="center">
   A functional wrapper around Spark to make it work with ZIO
</p>

## Documentation

You can find the documentation of zio-spark [here](https://univalence.github.io/zio-spark/).

## Help

You can ask us (Dylan, Jonathan) for some help if you want to use the lib or have questions around it : 
https://calendly.com/zio-spark/help

## Latest version

If you want to get the very last version of this library you can still download it using:

```scala
libraryDependencies += "io.univalence" %% "zio-spark" % "0.10.0"
```

### Snapshots

If you want to get the latest snapshots (the version associated with the last commit on master), you can still download
it using:

```scala
resolvers += Resolver.sonatypeRepo("snapshots"),
libraryDependencies += "io.univalence" %% "zio-spark" % "<SNAPSHOT-VERSION>"
```

You can find the latest version on 
[nexus repository manager](https://oss.sonatype.org/#nexus-search;gav~io.univalence~zio-spark_2.13~~~~kw,versionexpand).

### Spark Version

ZIO-Spark is compatible with Scala 2.11, 2.12 and 2.13. Spark is provided, you must add your own Spark version in 
build.sbt (as you would usually). 

We advise you to use the latest version of Spark for your scala version.

## news 🎉 zio-direct support 🎉

We worked to make zio-spark available for Scala 3, so it works with [zio-direct](https://github.com/zio/zio-direct).

```scala
import zio.*
import zio.direct.*
import zio.spark.sql.*

//import for syntax + spark encoders
import zio.spark.sql.implicits.*
import scala3encoders.given

//throwsAnalysisException directly
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

object Main extends ZIOAppDefault {
  val sparkSession = SparkSession.builder.master("local").asLayer

  override def run = {
    defer {
      val readBuild: RIO[SparkSession,DataFrame] = SparkSession.read.text("./build.sbt")
      val text: Dataset[String] = readBuild.run.as[String]

      text.filter(_.contains("zio")).show(truncate = false).run
      
      Console.printLine("what a time to be alive!").run
    }.provideLayer(sparkSession)
  }
}
```

build.sbt
```scala
scalaVersion := "3.2.1"

"dev.zio" %% "zio" % "2.0.5",
"dev.zio" % "zio-direct_3" % "1.0.0-RC1",
"io.univalence" %% "zio-spark" % "0.10.0",
("org.apache.spark" %% "spark-sql" % "3.3.1" % Provided).cross(CrossVersion.for3Use2_13),
("org.apache.hadoop" % "hadoop-client" % "3.3.1" % Provided),
"dev.zio" %% "zio-test" % "2.0.5" % Test
```

## Why ?

There are many reasons why we decide to build this library, such as:
* allowing user to build Spark pipeline with ZIO easily.
* making better code, pure FP, more composable, more readable Spark code.
* stopping the propagation of ```implicit SparkSessions```.
* improving some performances.
* taking advantage of ZIO allowing our jobs to retry and to be run in parallel.

## Alternatives

- [ZparkIO](https://github.com/leobenkel/ZparkIO) a framework for Spark, ZIO


## Spark with Scala3
- [iskra](https://github.com/VirtusLab/iskra) from VirtusLab, and interresting take and typesafety for Spark, without compromises on performance.
- [spark-scala3](https://github.com/vincenzobaz/spark-scala3), one of our dependency to support avec encoders for Spark in Scala3.


## Contributions

Pull requests are welcomed. We are open to organize pair-programming session to tackle improvements. If you want to add
new things in `zio-spark`, don't hesitate to open an issue!

You can also talk to us directly using this link if you are interested to contribute 
https://calendly.com/zio-spark/contribution.
