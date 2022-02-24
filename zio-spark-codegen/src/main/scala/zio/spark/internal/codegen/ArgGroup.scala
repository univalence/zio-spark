package zio.spark.internal.codegen

import scala.reflect.runtime.universe

case class ArgGroup(symbols: List[universe.Symbol]) {

  val parameters: List[Parameter] = symbols.map(Parameter.fromSymbol)

  def toCode(isArgs: Boolean, isHavingNullImplicitOrdering: Boolean = false): String = {
    val hasImplicit: Boolean = parameters.exists(_.isImplicit)

    parameters match {
      case Nil => if (isArgs) "()" else ""
      case _ =>
        if (isArgs && hasImplicit) ""
        else {
          val parameterCodes = parameters.map(_.toCode(isArgs, isHavingNullImplicitOrdering))
          val prefix         = if (hasImplicit) "(implicit " else "("
          prefix + parameterCodes.mkString(", ") + ")"
        }

    }
  }
}

object ArgGroup {
  def fromSymbol(symbols: List[universe.Symbol]): ArgGroup = ArgGroup(symbols: List[universe.Symbol])
}
