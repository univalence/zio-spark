package zio.spark.internal.codegen.structure

import scala.meta.*

case class TypeParameter(underlying: Type.Param) {
  def toCode: String = s"$underlying"
}

object TypeParameter {
  def fromScalaMeta(param: Type.Param): TypeParameter = TypeParameter(param)
}
