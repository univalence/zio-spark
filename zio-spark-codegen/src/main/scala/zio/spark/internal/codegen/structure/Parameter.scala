package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.ScalaBinaryVersion
import zio.spark.internal.codegen.structure.Parameter.Modifier

import scala.meta.*

case class Parameter(underlying: Term.Param, scalaVersion: ScalaBinaryVersion) {

  val name: String = underlying.name.toString

  val signature: String =
    scalaVersion match {
      case ScalaBinaryVersion.V2_13 => underlying.decltpe.get.toString.replace("TraversableOnce", "IterableOnce")
      case _                        => underlying.decltpe.get.toString
    }

  val maybeDefault: Option[String] = underlying.default.map(_.toString)

  val modifiers: Seq[Modifier] = if (underlying.collect { case d: Mod.Implicit => d }.nonEmpty) List(Modifier.Implicit) else Nil

  def toCode(isArgs: Boolean, className: String): String =
    if (isArgs) toCodeArgument(className)
    else toCodeParameter

  private def toCodeArgument(className: String): String =
    signature match {
      case _ if signature.contains("*")              => s"$name: _*"
      case _ if signature.startsWith(s"$className[") => s"$name.underlying"
      case _                                         => name
    }

  private def toCodeParameter: String =
    maybeDefault match {
      case Some(default) if default == "Utils.random.nextLong" =>
        s"$name: $signature" // TODO: Should be implemented with Random layer

      case Some(default) if default == "null" && name == "ord" => s"ord: $signature = noOrdering"
      case Some(default)                                       => s"$name: $signature = $default"
      case None                                                => s"$name: $signature"
    }

  def isImplicit: Boolean = modifiers.contains(Parameter.Modifier.Implicit)
}

object Parameter {
  sealed trait Modifier
  object Modifier {
    final case object Implicit extends Modifier
  }

  def fromScalaMeta(param: Term.Param, scalaVersion: ScalaBinaryVersion): Parameter = Parameter(param, scalaVersion)
}
