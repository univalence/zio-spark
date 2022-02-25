package zio.spark.internal.codegen.structure

import scala.meta.*

case class TypeParameter(underlying: Type.Param) {
  val name: String                = underlying.name.toString
  val contextBounds: List[String] = underlying.cbounds.map(_.toString)

  def toCode: String = {
    val unifiedContextBounds = if (contextBounds.isEmpty) "" else ": " + contextBounds.mkString(": ")
    s"$name$unifiedContextBounds"
  }
}

object TypeParameter {
  def fromScalaMeta(param: Type.Param): TypeParameter = TypeParameter(param)
}
