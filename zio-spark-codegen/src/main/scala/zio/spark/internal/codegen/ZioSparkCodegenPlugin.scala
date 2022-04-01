package zio.spark.internal.codegen

import org.scalafmt.interfaces.Scalafmt
import sbt.*
import sbt.Keys.*

import zio.{Console, ULayer, ZLayer}
import zio.spark.internal.codegen.generation.{Environment, Generator, Output}
import zio.spark.internal.codegen.generation.plan.Plan.*

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
        val version =
          scalaBinaryVersion.value match {
            case "2.11" => ScalaBinaryVersion.V2_11
            case "2.12" => ScalaBinaryVersion.V2_12
            case "2.13" => ScalaBinaryVersion.V2_13
          }

        val environment: ULayer[Environment] =
          ZLayer.succeed(
            Environment(
              mainFolder            = (Compile / scalaSource).value,
              classpath             = (Compile / dependencyClasspathAsJars).value,
              scalaVersion          = version,
              scalafmt              = Scalafmt.create(this.getClass.getClassLoader),
              scalafmtConfiguration = Paths.get(".scalafmt.conf")
            )
          )

        val generator: Generator =
          Generator(
            Seq(
              rddPlan,
              datasetPlan,
              dataFrameNaFunctionsPlan,
              dataFrameStatFunctionsPlan
            )
          )

        val outputs: Seq[Output] =
          zio.Runtime.default.unsafeRun(
            generator.generate
              .tapError(e => Console.printLine(e.forHuman))
              .provideCustomLayer(environment)
          )

        outputs.foreach(output => IO.write(output.file, output.code))

        outputs.map(_.file)
      }.taskValue
    )
}
