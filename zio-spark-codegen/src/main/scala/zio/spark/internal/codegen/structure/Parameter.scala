package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.structure.Parameter.Modifier

import scala.meta.*

case class Parameter(underlying: Term.Param) {

  val name: String                 = underlying.name.toString
  val signature: String            = underlying.decltpe.get.toString
  val maybeDefault: Option[String] = underlying.default.map(_.toString)

  val modifiers: Seq[Modifier] =
    if (underlying.collect { case d: Mod.Implicit => d }.nonEmpty) List(Modifier.Implicit) else Nil

  def toCode(isArgs: Boolean): String =
    if (isArgs) toCodeArgument
    else toCodeParameter

  private def toCodeArgument: String =
    if (signature.contains("*")) s"$name: _*"
    else name

  private def toCodeParameter: String =
    maybeDefault match {
      case Some(default) if default == "Utils.random.nextLong" =>
        s"$name: $signature" // TODO: Should be implemented with Random layer
      case Some(default) => s"$name: $signature = $default"
      case None          => s"$name: $signature"
    }

  def isImplicit: Boolean = modifiers.contains(Parameter.Modifier.Implicit)
}

object Parameter {
  sealed trait Modifier
  object Modifier {
    final case object Implicit extends Modifier
  }

  def fromScalaMeta(param: Term.Param): Parameter = Parameter(param)
}
