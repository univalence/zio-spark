package zio.spark.internal.codegen

import zio.spark.internal.codegen.Parameter.Modifier

import scala.reflect.runtime.universe

case class Parameter(symbol: universe.Symbol) {

  val name: String                     = symbol.name.toString
  private val signature: universe.Type = symbol.typeSignature

  val parameterType: String =
    if (signature.etaExpand.toString.contains("Class[")) signature.etaExpand.toString
    else TypeUtils.cleanType(signature.dealias.toString)

  val modifiers: Seq[Modifier] = if (symbol.isImplicit) List(Modifier.Implicit) else Nil

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

  def fromSymbol(symbol: universe.Symbol): Parameter = Parameter(symbol)
}
