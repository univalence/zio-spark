package zio.spark.internal.codegen.structure

object TypeUtils {

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

  def cleanType(type_ : String, path: String): String =
    cleanPrefixPackage(type_).replaceAll(",\\b", ", ") match {
      case "this.type" if path.contains("RDD")     => "RDD[T]"
      case "this.type" if path.contains("Dataset") => "Dataset[T]"
      case s                                       => s
    }
}
