package zio.spark.codegen.generation.plan

import sbt.Keys.Classpath
import sbt.fileToRichFile

import zio.{URIO, ZIO}
import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.{Logger, MethodType, Module}
import zio.spark.codegen.generation.Environment.{Environment, ZIOSparkFolders}
import zio.spark.codegen.generation.Error.{CodegenError, NotIndentedError, UnimplementedMethodsError}
import zio.spark.codegen.generation.Loader.{optionalSourceFromFile, sourceFromClasspath}
import zio.spark.codegen.generation.template.Template
import zio.spark.codegen.structure.{Method, SurroundedString}
import zio.spark.codegen.structure.Helpers.methodsFromSource
import zio.spark.codegen.structure.SurroundedString.*

import java.nio.file.Path

/**
 * A Spark Plan generate a ZIO Spark class combining tree kind of
 * sources:
 *   - The methods from Spark codebase
 *   - The methods from ZIO Spark overlay
 *   - The methods from ZIO Spark specific overlay
 */
case class SparkPlan(module: Module, template: Template) extends Plan { self =>
  val name: String = template.name

  def generateOverlayMethods: ZIO[ScalaBinaryVersion & ZIOSparkFolders, CodegenError, Seq[Method]] =
    for {
      scalaVersion    <- ZIO.service[ScalaBinaryVersion]
      zioSparkFolders <- ZIO.service[ZIOSparkFolders]
      overlayPath         = zioSparkFolders.itFolder / s"${name}Overlay.scala"
      overlaySpecificPath = zioSparkFolders.itFolderVersioned / s"${name}OverlaySpecific.scala"
      maybeOverlaySource         <- optionalSourceFromFile(overlayPath)
      maybeOverlaySpecificSource <- optionalSourceFromFile(overlaySpecificPath)
      overlayFunctions =
        maybeOverlaySource
          .map(src => methodsFromSource(src, filterOverlay = true, module.hierarchy, name, scalaVersion))
          .getOrElse(Seq.empty)
      overlaySpecificFunctions =
        maybeOverlaySpecificSource
          .map(src => methodsFromSource(src, filterOverlay = true, module.hierarchy, name, scalaVersion))
          .getOrElse(Seq.empty)
    } yield overlayFunctions ++ overlaySpecificFunctions

  def generateOverlayCode: ZIO[Environment, CodegenError, SurroundedString] =
    generateOverlayMethods.map { methods =>
      val code = methods.sortBy(_.name).map(_.raw).mkString("\n\n")
      "  // Handmade functions specific to zio-spark" <<< code
    }

  def prefixAllLines(text: String, prefix: String): String = text.split("\n").map(prefix + _).mkString("\n")

  def commentMethods(methods: String, title: String): String =
    s"""  // $title
       |  //
       |${prefixAllLines(methods, "  // ")}""".stripMargin

  def filterSparkFunctions(functions: Seq[Method]): Seq[Method] =
    functions
      .filterNot(_.name.contains("$"))
      .filterNot(_.name.contains("java.lang.Object"))
      .filterNot(_.name.contains("scala.Any"))
      .filterNot(_.name.contains("<init>"))
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("ju.")))   // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("jl.")))   // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("java")))  // Java specific implementation
      .filterNot(_.anyParameters.map(_.signature).exists(_.contains("Array"))) // Java specific implementation

  def getSparkMethods: ZIO[Logger & Classpath & ScalaBinaryVersion, CodegenError, Seq[Method]] =
    for {
      classpath    <- ZIO.service[Classpath]
      scalaVersion <- ZIO.service[ScalaBinaryVersion]
      sparkSource  <- sourceFromClasspath(s"${module.path}/$name.scala", module.name, classpath)
    } yield methodsFromSource(sparkSource, filterOverlay = false, module.hierarchy, name, scalaVersion)

  def generateSparkGroupedMethods: ZIO[Environment, CodegenError, Map[MethodType, Seq[Method]]] =
    for {
      sparkMethods <- getSparkMethods
      filteredFunctions = filterSparkFunctions(sparkMethods)
    } yield filteredFunctions
      .sortBy(_.name)
      .groupBy(template.getMethodType)

  def generateSparkCode: ZIO[Environment, CodegenError, SurroundedString] =
    for {
      groupedMethods <- generateSparkGroupedMethods
      code =
        groupedMethods.toList
          .sortBy(_._1)
          .map { case (methodType, methods) =>
            val sep =
              methodType match {
                case MethodType.ToImplement | MethodType.Ignored | MethodType.ToHandle => "\n"
                case _                                                                 => "\n\n"
              }

            val allMethods =
              methods
                .map(_.toCode(methodType, self))
                .distinct
                .mkString(sep)

            methodType match {
              case MethodType.ToImplement => commentMethods(allMethods, "Methods with handmade implementations")
              case MethodType.Ignored     => commentMethods(allMethods, "Ignored methods")
              case MethodType.ToHandle    => commentMethods(allMethods, "Methods that need to be implemented")
              case _                      => allMethods
            }
          }
          .mkString("\n\n  // ===============\n\n")
    } yield "  // Generated functions coming from spark" <<< code

  override def generateCode: ZIO[Environment, CodegenError, String] =
    for {
      scalaVersion <- ZIO.service[ScalaBinaryVersion]
      sparkCode    <- generateSparkCode
      overlayCode  <- generateOverlayCode
      maybeImports     = template.imports(scalaVersion)
      maybeAnnotations = template.annotations(scalaVersion)
      maybeImplicits   = template.implicits(scalaVersion).map("  // scalafix:off" <<< _ >>> "  // scalafix:on")
      definition       = template.definition
      helpers          = template.helpers
      code =
        s"""package ${module.zioHierarchy}
           |
           |${maybeImports.getOrElse("")}
           |
           |${maybeAnnotations.getOrElse("")}
           |$definition { self =>
           |${maybeImplicits.getOrElse("")}
           |
           |${helpers(name, template.typeParameters)}
           |
           |$overlayCode
           |
           |$sparkCode
           |}
           |""".stripMargin
    } yield code

  override def generatePath: URIO[ZIOSparkFolders, Path] =
    for {
      zioSparkFolders <- ZIO.service[ZIOSparkFolders]
      file = zioSparkFolders.mainFolderVersioned / module.zioPath / s"$name.scala"
    } yield file.toPath

  override def preValidation: ZIO[Environment, CodegenError, Unit] =
    for {
      groupedSourceMethods <- generateSparkGroupedMethods
      overlayMethods       <- generateOverlayMethods
      sourceMethodsToImplement     = groupedSourceMethods.getOrElse(MethodType.ToImplement, Seq.empty)
      overlayMethodNames           = overlayMethods.map(_.name).toSet
      sourceMethodNamesToImplement = sourceMethodsToImplement.map(_.name).toSet
      missingMethods               = sourceMethodNamesToImplement -- overlayMethodNames
    } yield if (missingMethods.isEmpty) ZIO.unit else ZIO.fail(UnimplementedMethodsError(missingMethods))

  override def postValidation(code: String): ZIO[Environment, CodegenError, Unit] =
    "\n//(.*?)\n".r.findAllIn(code).toList match {
      case Nil   => ZIO.unit
      case lines => ZIO.fail(NotIndentedError(lines))
    }
}
