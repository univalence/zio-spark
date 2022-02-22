package zio.spark.internal.codegen

import scala.reflect.runtime.universe

case class Call(parameters: List[Parameter]) {
  def toCode(isArgs: Boolean): String = {
    val hasImplicit = parameters.exists(_.isImplicit)

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

object Call {
  def fromSymbol(symbols: List[universe.Symbol]): Call =
    Call(
      parameters = symbols.map(Parameter.fromSymbol)
    )
}
