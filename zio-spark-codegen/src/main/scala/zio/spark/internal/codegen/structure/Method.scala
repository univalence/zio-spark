package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.{MethodType, ScalaBinaryVersion}
import zio.spark.internal.codegen.GenerationPlan.PlanType
import zio.spark.internal.codegen.structure.TypeUtils.*

import scala.meta.*
import scala.meta.contrib.AssociatedComments

case class Method(df: Defn.Def, comments: AssociatedComments, planType: PlanType, scalaVersion: ScalaBinaryVersion) {
  self =>

  val calls: List[ParameterGroup]    = df.paramss.map(pg => ParameterGroup.fromScalaMeta(pg, scalaVersion))
  val anyParameters: List[Parameter] = calls.flatMap(_.parameters)

  val name: String                   = df.name.value
  val returnType: String             = df.decltpe.get.toString()
  val fullName: String               = s"${planType.pkg}.$name"
  val typeParams: Seq[TypeParameter] = df.tparams.map(TypeParameter.fromScalaMeta)

  val comment =
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
      case MethodType.Ignored     => s"[[$fullName]]"
      case MethodType.ToImplement => s"[[$fullName]]"
      case MethodType.TODO        => s"[[$fullName]]"
      case _ =>
        val parameters = calls.map(_.toCode(isArgs = false)).mkString("")
        val arguments  = calls.map(_.toCode(isArgs = true)).mkString("")

        val transformation =
          methodType match {
            case MethodType.DriverAction               => "action"
            case MethodType.DistributedComputation     => "action"
            case MethodType.GetWithAnalysis            => "getWithAnalysis"
            case MethodType.TransformationWithAnalysis => "transformationWithAnalysis"
            case MethodType.Transformation             => "transformation"
            case _                                     => "get"
          }

        val cleanReturnType = cleanType(returnType, planType.pkg)

        val trueReturnType =
          methodType match {
            case MethodType.DriverAction               => s"Task[$cleanReturnType]"
            case MethodType.DistributedComputation     => s"Task[$cleanReturnType]"
            case MethodType.GetWithAnalysis            => s"TryAnalysis[$cleanReturnType]"
            case MethodType.TransformationWithAnalysis => s"TryAnalysis[$cleanReturnType]"
            case _                                     => cleanReturnType
          }

        val strTypeParams = if (typeParams.nonEmpty) s"[${typeParams.map(_.toCode).mkString(", ")}]" else ""

        val conversion = if (returnType.startsWith("Array")) ".toSeq" else ""

        val deprecation: String =
          df.collect { case d: Mod.Annot if d.toString.contains("deprecated") => d }
            .headOption
            .map(_.toString)
            .getOrElse("")

        s"""$comment$deprecation
           |def $name$strTypeParams$parameters: $trueReturnType = $transformation(_.$name$arguments$conversion)""".stripMargin
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
      planType: PlanType,
      scalaVersion: ScalaBinaryVersion
  ): Method = Method(df, comments, planType, scalaVersion)
}
