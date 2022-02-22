package zio.spark.internal.codegen

import scala.reflect.runtime.universe

case class Parameter(name: String, parameterType: String, modifiers: List[Parameter.Modifier]) {
  def toCode(isArgs: Boolean): String =
    if (isArgs) name
    else s"$name: $parameterType"

  def isImplicit: Boolean = modifiers.contains(Parameter.Modifier.Implicit)
}

object Parameter {
  sealed trait Modifier
  object Modifier {
    final case object Implicit extends Modifier
  }

  def fromSymbol(symbol: universe.Symbol): Parameter =
    Parameter(
      name          = symbol.name.toString,
      parameterType = symbol.typeSignature.typeSymbol.name.toString,
      modifiers =
        if (symbol.isImplicit) List(Modifier.Implicit)
        else Nil
    )
}
