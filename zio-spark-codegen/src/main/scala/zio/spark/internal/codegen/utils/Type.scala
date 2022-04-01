package zio.spark.internal.codegen.utils

object Type {
  def cleanPrefixPackage(type_ : String): String = {
    import scala.meta.*

    val res =
      type_
        .parse[Type]
        .get
        .transform {
          case t"Array"                                 => t"Seq"
          case Type.Select(q"scala.collection", tpname) => t"collection.$tpname"
          case t"$ref.$tpname"                          => tpname
        }
    res.toString()
  }

  def cleanType(type_ : String): String = cleanPrefixPackage(type_).replaceAll(",\\b", ", ")
}
