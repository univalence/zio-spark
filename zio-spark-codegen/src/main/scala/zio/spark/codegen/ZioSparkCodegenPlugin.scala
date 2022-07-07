package zio.spark.codegen

import org.scalafmt.interfaces.Scalafmt
import sbt.{settingKey, AutoPlugin, Compile, Def}
import sbt.Keys.*

import zio.{ULayer, Unsafe, URLayer, ZIO, ZLayer}
import zio.spark.codegen.generation.{Generator, Logger, Output}
import zio.spark.codegen.generation.Environment.{
  ScalafmtFormatter,
  ScalafmtFormatterLive,
  ZIOSparkFolders,
  ZIOSparkFoldersLive
}
import zio.spark.codegen.generation.plan.Plan
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

        val plans: Seq[Plan] =
          Seq(
            rddPlan,
            datasetPlan,
            dataFrameNaFunctionsPlan,
            dataFrameStatFunctionsPlan,
            relationalGroupedDatasetPlan,
            keyValueGroupedDatasetPlan
          )

        Unsafe.unsafeCompat { implicit u =>
          val outputs: Seq[Output] =
            zio.Runtime.default.unsafe
              .run(
                Generator
                  .generateAll(plans)
                  .provide(
                    scalaVersionLayer,
                    classpathLayer,
                    scalafmtLayer,
                    zioSparkFoldersLayer,
                    Logger.live
                  )
              )
              .getOrThrowFiberFailure()

          outputs.map(_.file)
        }
      }.taskValue
    )
}
