package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.ScalaBinaryVersion
import zio.spark.internal.codegen.generation.MethodType
import zio.spark.internal.codegen.utils.Type.*

import scala.meta.*
import scala.meta.contrib.AssociatedComments

case class Method(
    df:           Defn.Def,
    comments:     AssociatedComments,
    hierarchy:    String,
    className:    String,
    scalaVersion: ScalaBinaryVersion
) {
  self =>

  val calls: List[ParameterGroup]    = df.paramss.map(pg => ParameterGroup.fromScalaMeta(pg, scalaVersion))
  val anyParameters: List[Parameter] = calls.flatMap(_.parameters)

  val name: String                   = df.name.value
  val returnType: String             = df.decltpe.get.toString()
  val fullName: String               = s"$hierarchy.$className.$name"
  val typeParams: Seq[TypeParameter] = df.tparams.map(TypeParameter.fromScalaMeta)

  val comment: String =
    if (comments.leading(df).isEmpty) ""
    else
      comments
        .leading(df)
        .mkString("  ", "\n  ", "")
        .replace("numPartitions = 1", "{{{ numPartitions = 1 }}}")
        .replace("(Scala-specific) ", "")

  val raw: String =
    s"""$comment
       |$df""".stripMargin

  def toCode(methodType: MethodType): String =
    methodType match {
      case MethodType.Ignored | MethodType.ToImplement | MethodType.ToHandle => s"[[$fullName]]"
      case _ =>
        val effectful: Boolean =
          methodType match {
            case MethodType.DriverAction | MethodType.DistributedComputation => true
            case _                                                           => false
          }

        val parameters = {
          val sparkParameters = calls.map(_.toCode(isArgs = false, effectful = effectful, className)).mkString("")

          calls match {
            case list if effectful && !list.exists(_.hasImplicit) => s"$sparkParameters(implicit trace: ZTraceElement)"
            case _                                                => sparkParameters
          }
        }

        val arguments = calls.map(_.toCode(isArgs = true, effectful = false, className)).mkString("")

        val transformation =
          methodType match {
            case MethodType.DriverAction               => "action"
            case MethodType.DistributedComputation     => "action"
            case MethodType.GetWithAnalysis            => "getWithAnalysis"
            case MethodType.TransformationWithAnalysis => "transformationWithAnalysis"
            case MethodType.Transformation             => "transformation"
            case MethodType.UnpackWithAnalysis         => "unpackWithAnalysis"
            case MethodType.Unpack                     => "unpack"
            case _                                     => "get"
          }

        val cleanReturnType = cleanType(returnType)

        val trueReturnType =
          methodType match {
            case MethodType.DriverAction               => s"Task[$cleanReturnType]"
            case MethodType.DistributedComputation     => s"Task[$cleanReturnType]"
            case MethodType.GetWithAnalysis            => s"TryAnalysis[$cleanReturnType]"
            case MethodType.TransformationWithAnalysis => s"TryAnalysis[$cleanReturnType]"
            case MethodType.UnpackWithAnalysis         => s"TryAnalysis[$cleanReturnType]"
            case _                                     => cleanReturnType
          }

        val strTypeParams: Boolean => String =
          inDefinition => if (typeParams.nonEmpty) s"[${typeParams.map(_.toCode(inDefinition)).mkString(", ")}]" else ""

        val defTypeParams  = strTypeParams(true)
        val bodyTypeParams = strTypeParams(false)

        val conversion = if (returnType.startsWith("Array")) ".toSeq" else ""

        val deprecation: String =
          df.collect { case d: Mod.Annot if d.toString.contains("deprecated") => d }
            .headOption
            .map(_.toString)
            .getOrElse("")

        s"""$comment$deprecation
           |def $name$defTypeParams$parameters: $trueReturnType = 
           |  $transformation(_.$name$bodyTypeParams$arguments$conversion)""".stripMargin
    }

  def isSetter: Boolean = name.startsWith("set")
}

object Method {
  sealed trait Kind
  object Kind {
    final case object Function extends Kind
    final case object Setter   extends Kind
  }

  def fromScalaMeta(
      df: Defn.Def,
      comments: AssociatedComments,
      hierarchy: String,
      className: String,
      scalaVersion: ScalaBinaryVersion
  ): Method = Method(df, comments, hierarchy, className, scalaVersion)
}
