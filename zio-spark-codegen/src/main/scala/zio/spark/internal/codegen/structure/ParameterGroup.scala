package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.ScalaBinaryVersion

import scala.meta.*

case class ParameterGroup(underlying: Seq[Term.Param], scalaVersion: ScalaBinaryVersion) {

  val parameters: Seq[Parameter] = underlying.map(p => Parameter.fromScalaMeta(p, scalaVersion))

  def toCode(isArgs: Boolean): String = {
    val hasImplicit: Boolean = parameters.exists(_.isImplicit)

    parameters match {
      case Nil => if (isArgs) "()" else ""
      case _ =>
        if (isArgs && hasImplicit) ""
        else {
          val parameterCodes = parameters.map(_.toCode(isArgs))
          val prefix         = if (hasImplicit) "(implicit " else "("
          prefix + parameterCodes.mkString(", ") + ")"
        }

    }
  }
}

object ParameterGroup {
  def fromScalaMeta(params: Seq[Term.Param], scalaVersion: ScalaBinaryVersion): ParameterGroup = ParameterGroup(params, scalaVersion)
}
