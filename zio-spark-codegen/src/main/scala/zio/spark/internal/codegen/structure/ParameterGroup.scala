package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.ScalaBinaryVersion

import scala.meta.*

case class ParameterGroup(underlying: Seq[Term.Param], scalaVersion: ScalaBinaryVersion) {

  val parameters: Seq[Parameter] = underlying.map(p => Parameter.fromScalaMeta(p, scalaVersion))

  val hasImplicit: Boolean = parameters.exists(_.isImplicit)

  def toCode(isArgs: Boolean, effectful: Boolean, className: String): String =
    parameters match {
      case Nil => if (isArgs) "()" else ""
      case _ =>
        if (isArgs && hasImplicit) ""
        else {
          val parameterCodes    = parameters.map(_.toCode(isArgs, callByName = effectful && !hasImplicit, className))
          val parametersUnified = parameterCodes.mkString(", ")
          (hasImplicit, effectful) match {
            case (true, true)  => s"(implicit $parametersUnified, trace: ZTraceElement)"
            case (true, false) => s"(implicit $parametersUnified)"
            case _             => s"($parametersUnified)"
          }
        }

    }
}

object ParameterGroup {
  def fromScalaMeta(params: Seq[Term.Param], scalaVersion: ScalaBinaryVersion): ParameterGroup = ParameterGroup(params, scalaVersion)
}
