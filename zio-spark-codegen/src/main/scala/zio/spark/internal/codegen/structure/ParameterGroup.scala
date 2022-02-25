package zio.spark.internal.codegen.structure

import scala.meta.*

case class ParameterGroup(underlying: Seq[Term.Param]) {

  val parameters: Seq[Parameter] = underlying.map(Parameter.fromScalaMeta)

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
  def fromScalaMeta(params: Seq[Term.Param]): ParameterGroup = ParameterGroup(params)
}
