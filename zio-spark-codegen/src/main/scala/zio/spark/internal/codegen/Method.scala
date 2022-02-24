package zio.spark.internal.codegen

import zio.spark.internal.codegen.Method.Kind
import zio.spark.internal.codegen.RDDAnalysis.MethodType
import zio.spark.internal.codegen.TypeUtils.*

import scala.reflect.runtime.universe

object TypeUtils {

  def cleanPrefixPackage(type_ : String): String = {
    import scala.meta.*

    val importedPackages =
      Seq(
        "scala.reflect",
        "scala.math",
        "scala",
        "java.lang",
        "org.apache.spark.rdd",
        "org.apache.spark",
        "org.apache.spark.partial"
      )

    val res =
      type_
        .parse[Type]
        .get
        .transform {
          case t"Array"                                 => t"Seq"
          case Type.Select(q"scala.collection", tpname) => t"collection.$tpname"
          case t"$ref.$tpname" if importedPackages.contains(ref.toString()) =>
            tpname
        }
    res.toString()
  }

  def cleanType(type_ : String): String =
    cleanPrefixPackage(type_).replaceAll(",\\b", ", ") match {
      case "RDD.this.type" => "RDD[T]"
      case s               => s
    }
}

case class Method(symbol: universe.MethodSymbol) {

  private val calls: List[ArgGroup]   = symbol.paramLists.map(ArgGroup.fromSymbol)
  private val kind                    = if (symbol.isSetter) Kind.Setter else Kind.Function
  val name: String                    = symbol.name.toString
  val annotations: Seq[String]        = symbol.annotations.map(_.toString)
  val path: String                    = symbol.fullName.split('.').dropRight(1).mkString(".")
  val returnType: universe.TypeSymbol = symbol.returnType.typeSymbol.asType
  val fullName: String                = s"$path.$name"
  val typeParams: Seq[String]         = symbol.typeParams.map(_.name.toString)

  def toCode(methodType: MethodType): String =
    methodType match {
      case MethodType.Ignored     => s"[[$fullName]]"
      case MethodType.ToImplement => s"[[$fullName]]"
      case _ =>
        val parameters = calls.map(_.toCode(false)).mkString("")
        val arguments  = calls.map(_.toCode(true)).mkString("")

        val transformation =
          methodType match {
            case MethodType.DriverAction           => "action"
            case MethodType.DistributedComputation => "action"
            case MethodType.Transformation         => "transformation"
            case _                                 => "succeedNow"
          }

        val cleanReturnType = cleanType(symbol.returnType.dealias.toString)

        val trueReturnType =
          methodType match {
            case MethodType.DriverAction           => s"Task[$cleanReturnType]"
            case MethodType.DistributedComputation => s"Task[$cleanReturnType]"
            case _                                 => cleanReturnType
          }

        val strTypeParams = if (typeParams.nonEmpty) s"[${typeParams.mkString(", ")}]" else ""

        s"def $name$strTypeParams$parameters: $trueReturnType = $transformation(_.$name$arguments)"
    }

  def isSetter: Boolean = kind == Method.Kind.Setter
}

object Method {
  sealed trait Kind
  object Kind {
    final case object Function extends Kind
    final case object Setter   extends Kind
  }

  def fromSymbol(symbol: universe.MethodSymbol): Method = Method(symbol)
}
