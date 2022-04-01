package zio.spark.codegen.structure

import scala.meta.*

case class TypeParameter(underlying: Type.Param) {
  def toCode(inDefinition: Boolean): String =
    if (inDefinition) {
      s"$underlying"
    } else {
      val pattern = "([A-Za-z0-9]+)".r
      pattern.findFirstIn(s"$underlying").get
    }
}

object TypeParameter {
  def fromScalaMeta(param: Type.Param): TypeParameter = TypeParameter(param)
}
