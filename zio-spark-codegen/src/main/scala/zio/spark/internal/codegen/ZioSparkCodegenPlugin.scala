package zio.spark.internal.codegen

import org.scalafmt.interfaces.Scalafmt
import sbt.*
import sbt.Keys.*

import zio.spark.internal.codegen.MethodType.*

import scala.collection.immutable

import java.nio.file.*

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
            val scalaDir: File = (Compile / scalaSource).value
            new File(scalaDir.getPath + "-" + scalaBinaryVersion.value) / "zio" / "spark" / "internal" / "codegen" / s"Base${plan.name}.scala"
          }

        /**
         * Checks that all methods that need to be implemented are
         * indeed implemented in zio-spark.
         *
         * It throws an exception if one of them is not implemented.
         *
         * @param plans
         *   The plans
         */
        def checkAllMethodsAreImplemented(plans: Seq[GenerationPlan]): Unit = {
          val plansWithMissingMethods: Seq[(String, Set[String])] =
            plans.map { plan =>
              val allMethods                  = plan.getFinalClassMethods((Compile / scalaSource).value)
              val methodsToImplement          = plan.methodsWithTypes.getOrElse(MethodType.ToImplement, Seq.empty).map(_.name).toSet
              val missingMethods: Set[String] = methodsToImplement -- allMethods
              plan.name -> missingMethods
            }

          val plansWithMissingMethodsNonEmpty = plansWithMissingMethods.filter(_._2.nonEmpty)

          if (plansWithMissingMethodsNonEmpty.nonEmpty) {
            val introduction = "The following methods should be implemented:"

            val explication =
              """If this method should be implemented later on 
                |consider identifying this method as 'Todo' and 
                |not 'ToImplement' using 'getMethodType' function.""".stripMargin.replace("\n", "")

            val errors =
              plansWithMissingMethodsNonEmpty
                .map { case (name, missingMethods) =>
                  val missingMethodsStr = missingMethods.map(" - " + _).mkString("\n")
                  s"""  $name:
                     |  $missingMethodsStr""".stripMargin
                }
                .mkString("\n")

            throw new NotImplementedError(s"""
                                             |$introduction
                                             |$errors
                                             |$explication""".stripMargin)
          }
        }

        generationPlans.zip(generatedFiles).foreach { case (plan, file) =>
          val methodsWithMethodTypes = plan.methodsWithTypes

          val body: String =
            methodsWithMethodTypes.toList
              .sortBy(_._1)
              .map { case (methodType, methods) =>
                val sep =
                  methodType match {
                    case MethodType.ToImplement => "\n"
                    case MethodType.Ignored     => "\n"
                    case MethodType.TODO        => "\n"
                    case _                      => "\n\n"
                  }

                val allMethods = methods.sortBy(_.fullName).map(_.toCode(methodType)).distinct.mkString(sep)

                methodType match {
                  case MethodType.ToImplement => commentMethods(allMethods, "Methods with handmade implementations")
                  case MethodType.Ignored     => commentMethods(allMethods, "Ignored methods")
                  case MethodType.TODO        => commentMethods(allMethods, "Methods that need to be implemented")
                  case _                      => allMethods
                }
              }
              .mkString("\n\n//===============\n\n")

          val scalafmt = Scalafmt.create(this.getClass.getClassLoader)
          val config   = Paths.get(".scalafmt.conf")

          val code =
            s"""/** 
               | * /!\\ Warning /!\\
               | *
               | * This file is generated using zio-spark-codegen, you should not edit 
               | * this file directly.
               | */
               |
               |package zio.spark.internal.codegen
               |
               |${plan.imports}
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

          val formattedCode = scalafmt.format(config, file.toPath, code)

          IO.write(file, formattedCode)
        }

        checkAllMethodsAreImplemented(generationPlans)

        generatedFiles
      }.taskValue
    )
}
