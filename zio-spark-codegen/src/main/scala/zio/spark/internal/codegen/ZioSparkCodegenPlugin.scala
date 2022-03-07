package zio.spark.internal.codegen

import org.scalafmt.interfaces.Scalafmt
import sbt.*
import sbt.Keys.*
import zio.spark.internal.codegen.GenerationPlan.PlanType
import zio.spark.internal.codegen.MethodType.*

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
        val classpath: Classpath = (Compile / dependencyClasspathAsJars).value

        val version =
          scalaBinaryVersion.value match {
            case "2.11" => ScalaBinaryVersion.V2_11
            case "2.12" => ScalaBinaryVersion.V2_12
            case "2.13" => ScalaBinaryVersion.V2_13
          }

        val planTypes: Seq[PlanType] =
          List(
            GenerationPlan.RDDPlan,
            GenerationPlan.DatasetPlan,
            GenerationPlan.DataFrameNaFunctionsPlan
          )

        val generationPlans = planTypes.map(_.getGenerationPlan(classpath, version)).map(zio.Runtime.default.unsafeRun)

        val generatedFiles =
          generationPlans.map { plan =>
            val scalaDir: File = (Compile / scalaSource).value
            val basePath: File = new File(scalaDir.getPath + "-" + scalaBinaryVersion.value)
            basePath / "zio" / "spark" / "internal" / "codegen" / s"${plan.planType.outputName}.scala"
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
            plans.filter(_.planType.fold(true, true, false)).map { plan =>
              val allMethods                  = plan.getFinalClassMethods((Compile / scalaSource).value)
              val methodsToImplement          = plan.methodsWithTypes.getOrElse(MethodType.ToImplement, Seq.empty).map(_.name).toSet
              val missingMethods: Set[String] = methodsToImplement -- allMethods
              plan.planType.name -> missingMethods
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

        /**
         * Checks that formatted code doesn't contain non indented
         * single line.
         * @param code
         *   The code to check
         */
        def checkPostFormatting(code: String): Unit = {
          val check: Option[String] = "\n//(.*?)\n".r.findFirstIn(code)
          check.foreach { line =>
            throw new AssertionError(s"generated file should not contain non-indented single-line comments $line")
          }
        }

        val scalafmt = Scalafmt.create(this.getClass.getClassLoader)
        val config   = Paths.get(".scalafmt.conf")

        generationPlans.zip(generatedFiles).foreach { case (plan, file) =>
          val body: String =
            plan.methodsWithTypes.toList
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
              .mkString("\n\n  // ===============\n\n")

          val sourceCode: String    = plan.planType.sourceCode(body, version)
          val formattedCode: String = scalafmt.format(config, file.toPath, sourceCode)

          checkPostFormatting(formattedCode)

          IO.write(file, formattedCode)
        }
        checkAllMethodsAreImplemented(generationPlans)
        generatedFiles
      }.taskValue
    )
}
