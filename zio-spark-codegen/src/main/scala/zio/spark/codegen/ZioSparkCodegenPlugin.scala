package zio.spark.codegen

import org.scalafmt.interfaces.Scalafmt
import sbt.*
import sbt.Keys.*

import zio.{Console, ULayer, URLayer, ZIO, ZLayer}
import zio.spark.codegen.generation.{Generator, Output}
import zio.spark.codegen.generation.Environment.{ScalafmtFormatter, ScalafmtFormatterLive, ZIOSparkFolders, ZIOSparkFoldersLive}
import zio.spark.codegen.generation.plan.Plan.*

import java.nio.file.*

object ZioSparkCodegenPlugin extends AutoPlugin {
  object autoImport {
    val sparkLibraryVersion = settingKey[String]("Specifies the version of Spark to depend on")
  }

  def prefixAllLines(text: String, prefix: String): String = text.split("\n").map(prefix + _).mkString("\n")

  def commentMethods(methods: String, title: String): String =
    s"""  // $title
       |  //
       |${prefixAllLines(methods, "  // ")}""".stripMargin

  override lazy val projectSettings =
    Seq(
      Compile / sourceGenerators += Def.task {
        val version: ScalaBinaryVersion =
          scalaBinaryVersion.value match {
            case "2.11" => ScalaBinaryVersion.V2_11
            case "2.12" => ScalaBinaryVersion.V2_12
            case "2.13" => ScalaBinaryVersion.V2_13
          }

        val scalaVersionLayer: ULayer[ScalaBinaryVersion] = ZLayer.succeed(version)
        val classpathLayer: ULayer[Classpath]             = ZLayer.succeed((Compile / dependencyClasspathAsJars).value)
        val scalafmtLayer: ULayer[ScalafmtFormatter] =
          ZLayer.succeed(
            ScalafmtFormatterLive(
              Scalafmt.create(this.getClass.getClassLoader),
              Paths.get(".scalafmt.conf")
            )
          )
        val zioSparkFoldersLayer: URLayer[ScalaBinaryVersion, ZIOSparkFolders] =
          ZLayer {
            for {
              scalaVersion <- ZIO.service[ScalaBinaryVersion]
            } yield ZIOSparkFoldersLive((Compile / scalaSource).value, scalaVersion)
          }

        val generator: Generator =
          Generator(
            Seq(
              rddPlan,
              datasetPlan,
              dataFrameNaFunctionsPlan,
              dataFrameStatFunctionsPlan,
              relationalGroupedDatasetPlan,
              keyValueGroupedDatasetPlan
            )
          )

        val outputs: Seq[Output] =
          zio.Runtime.default.unsafeRun(
            generator.generate
              .tapError(e => Console.printLine(e.forHuman))
              .provide(scalaVersionLayer, classpathLayer, scalafmtLayer, zioSparkFoldersLayer, Console.live)
          )

        outputs.foreach(output => IO.write(output.file, output.code))

        outputs.map(_.file)
      }.taskValue
    )
}
