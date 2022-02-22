package zio.spark.internal.codegen

import scala.reflect.runtime.universe

case class Call(parameters: List[Parameter]) {
  def toCode(isArgs: Boolean): String = {
    val params      = parameters.filterNot(_.isImplicit).map(_.toCode(isArgs))
    val hasImplicit = parameters.exists(_.isImplicit)

    params match {
      case Nil => if (isArgs) "()" else ""
      case _ =>
        val prefix = if (hasImplicit) "(implicit " else "("
        prefix + params.mkString(", ") + ")"
    }
  }
}

object Call {
  def fromSymbol(symbols: List[universe.Symbol]): Call =
    Call(
      parameters = symbols.map(Parameter.fromSymbol)
    )
}
