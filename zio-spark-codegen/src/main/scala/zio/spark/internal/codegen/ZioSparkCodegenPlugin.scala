package zio.spark.internal.codegen

import sbt.*
import sbt.Keys.*

import zio.spark.internal.codegen.MethodType.*

import scala.collection.immutable

object ZioSparkCodegenPlugin extends AutoPlugin {
  object autoImport {
    val sparkLibraryVersion = settingKey[String]("Specifies the version of Spark to depend on")
  }

  def prefixAllLines(text: String, prefix: String): String = text.split("\n").map(prefix + _).mkString("\n")

  def commentMethods(methods: String, title: String): String =
    s"""/**
       | * $title
       | *
       |${prefixAllLines(methods, " * ")}
       | */""".stripMargin

  override lazy val projectSettings =
    Seq(
      Compile / sourceGenerators += Def.task {
        val jars = (Compile / dependencyClasspathAsJars).value

        val generationPlans: immutable.Seq[GenerationPlan] =
          List(
            GenerationPlan.rddPlan(jars),
            GenerationPlan.datasetPlan(jars)
          ).map(zio.Runtime.default.unsafeRun)

        val generatedFiles =
          generationPlans.map { plan =>
            (Compile / scalaSource).value / "zio" / "spark" / "internal" / "codegen" / s"Base${plan.name}.scala"
          }

        generationPlans.zip(generatedFiles).foreach { case (plan, file) =>
          val methods =
            plan.methods
              .filterNot(_.fullName.contains("$"))
              .filterNot(_.fullName.contains("java.lang.Object"))
              .filterNot(_.fullName.contains("scala.Any"))
              .filterNot(_.fullName.contains("<init>"))

          val methodsWithMethodTypes = methods.groupBy(getMethodType)

          val body: String =
            methodsWithMethodTypes.toList
              .sortBy(_._1)
              .map { case (methodType, methods) =>
                val sep =
                  methodType match {
                    case MethodType.ToImplement => "\n"
                    case MethodType.Ignored     => "\n"
                    case _                      => "\n\n"
                  }

                val allMethods = methods.sortBy(_.fullName).map(_.toCode(methodType)).distinct.mkString(sep)
                methodType match {
                  case MethodType.ToImplement => commentMethods(allMethods, "Methods to implement")
                  case MethodType.Ignored     => commentMethods(allMethods, "Ignored method")
                  case _                      => allMethods
                }
              }
              .mkString("\n\n//===============\n\n")

          IO.write(
            file,
            s"""package zio.spark.internal.codegen
               |
               |${plan.imports}
               |
               |
               |@SuppressWarnings(Array("scalafix:DisableSyntax.defaultArgs", "scalafix:DisableSyntax.null"))
               |abstract class Base${plan.name}[T](underlying${plan.name}: ImpureBox[Underlying${plan.name}[T]]) extends Impure[Underlying${plan.name}[T]](underlying${plan.name}) {
               |  import underlying${plan.name}._
               |
               |${prefixAllLines(plan.baseImplicits, "  ")}
               |  
               |${prefixAllLines(plan.helpers, "  ")}
               |
               |${prefixAllLines(body, "  ")}
               |}
               |""".stripMargin
          )
        }

        generatedFiles
      }.taskValue
    )
}
