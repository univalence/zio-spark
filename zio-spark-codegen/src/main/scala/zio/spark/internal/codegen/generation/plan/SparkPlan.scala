package zio.spark.internal.codegen.generation.plan

import sbt.*

import zio.{Console, URIO, ZIO}
import zio.spark.internal.codegen.generation.{Environment, MethodType, Module}
import zio.spark.internal.codegen.generation.Error.{CodegenError, NotIndentedError, UnimplementedMethodsError}
import zio.spark.internal.codegen.generation.Loader.{optionalSourceFromFile, sourceFromClasspath}
import zio.spark.internal.codegen.generation.template.Template
import zio.spark.internal.codegen.structure.Method
import zio.spark.internal.codegen.utils.Meta.functionsFromSource

import java.nio.file.Path

/**
 * A Spark Plan generate a ZIO Spark class combining tree kind of
 * sources:
 *   - The methods from Spark codebase
 *   - The methods from ZIO Spark overlay
 *   - The methods from ZIO Spark specific overlay
 */
case class SparkPlan(module: Module, template: Template) extends Plan {
  val name: String = template.name

  def generateOverlayMethods: ZIO[Console & Environment, CodegenError, Seq[Method]] =
    for {
      environment <- ZIO.service[Environment]
      overlayPath         = environment.itFolder / s"${name}Overlay.scala"
      overlaySpecificPath = environment.itFolderVersioned / s"${name}OverlaySpecific.scala"
      maybeOverlaySource         <- optionalSourceFromFile(overlayPath)
      maybeOverlaySpecificSource <- optionalSourceFromFile(overlaySpecificPath)
      overlayFunctions =
        maybeOverlaySource
          .map(src => functionsFromSource(src, filterOverlay = true, module.hierarchy, name, environment.scalaVersion))
          .getOrElse(Seq.empty)
      overlaySpecificFunctions =
        maybeOverlaySpecificSource
          .map(src => functionsFromSource(src, filterOverlay = true, module.hierarchy, name, environment.scalaVersion))
          .getOrElse(Seq.empty)
    } yield overlayFunctions ++ overlaySpecificFunctions

  def generateOverlayCode: ZIO[Console & Environment, CodegenError, String] =
    generateOverlayMethods.map(methods =>
      methods.sortBy(_.name).map(_.raw).mkString("\n\n") match {
        case code if code.nonEmpty =>
          s"""  // Handmade functions specific to zio-spark
             |
             |$code""".stripMargin
        case _ => ""
      }
    )

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

  def generateSparkGroupedMethods: ZIO[Console & Environment, CodegenError, Map[MethodType, Seq[Method]]] =
    for {
      environment <- ZIO.service[Environment]
      sparkSource <- sourceFromClasspath(s"${module.path}/$name.scala", module.name, environment.classpath)
      allFunctions =
        functionsFromSource(sparkSource, filterOverlay = false, module.hierarchy, name, environment.scalaVersion)
      filteredFunctions = filterSparkFunctions(allFunctions)
    } yield filteredFunctions
      .sortBy(_.name)
      .groupBy(template.getMethodType)

  def generateSparkCode: ZIO[Console & Environment, CodegenError, String] =
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
                .map(_.toCode(methodType))
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
    } yield code match {
      case _ if code.nonEmpty =>
        s"""  // Generated functions coming from spark
           |
           |$code""".stripMargin
      case _ => ""
    }

  override def generateCode: ZIO[Console & Environment, CodegenError, String] =
    for {
      environment <- ZIO.service[Environment]
      sparkCode   <- generateSparkCode
      overlayCode <- generateOverlayCode
      maybeImports     = template.imports(environment)
      maybeAnnotations = template.annotations(environment)
      maybeImplicits   = template.implicits(environment)
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
    } yield postProcess(code)

  final def postProcess(code: String): String = code.replace("this.type", s"$name${template.typeParameter}")

  override def generatePath: URIO[Environment, Path] =
    for {
      environment <- ZIO.service[Environment]
      file = environment.mainFolderVersioned / module.zioPath / s"$name.scala"
    } yield file.toPath

  override def preValidation: ZIO[Console & Environment, CodegenError, Unit] =
    for {
      groupedSourceMethods <- generateSparkGroupedMethods
      overlayMethods       <- generateOverlayMethods
      sourceMethodsToImplement     = groupedSourceMethods.getOrElse(MethodType.ToImplement, Seq.empty)
      overlayMethodNames           = overlayMethods.map(_.name).toSet
      sourceMethodNamesToImplement = sourceMethodsToImplement.map(_.name).toSet
      missingMethods               = sourceMethodNamesToImplement -- overlayMethodNames
    } yield if (missingMethods.isEmpty) ZIO.unit else ZIO.fail(UnimplementedMethodsError(missingMethods))

  override def postValidation(code: String): ZIO[Console & Environment, CodegenError, Unit] =
    if ("\n//(.*?)\n".r.findFirstIn(code).isEmpty) ZIO.unit else ZIO.fail(NotIndentedError)

}
