package zio.spark.internal.codegen

import zio.spark.internal.codegen.Method.Kind
import zio.spark.internal.codegen.RDDAnalysis.MethodType
import zio.spark.internal.codegen.TypeUtils.{cleanType, cleanTypeSymbol}

import scala.reflect.runtime.universe

object TypeUtils {

  def cleanTypeSymbol(typeSymbol: universe.TypeSymbol): String =
    typeSymbol.fullName
      .replaceAll("^scala\\.", "")
      .replaceAll("^java\\.lang\\.", "")

  def cleanType(type_ : universe.Type): String =
    type_.etaExpand.toString.replaceAll("^\\[T\\]", "") match {
      case "org.apache.spark.rdd.RDD.T" => "T"
      case s                            => s
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

  def toCode(methodType: MethodType): String =
    methodType match {
      case MethodType.Ignored     => s"[[$fullName]]"
      case MethodType.ToImplement => s"[[$fullName]]"
      case _ =>
        val parameters = calls.map(_.toCode(false)).mkString("")
        val arguments  = calls.map(_.toCode(true)).mkString("")

        val transformation =
          methodType match {
            case MethodType.DriverAction           => "attemptBlocking"
            case MethodType.DistributedComputation => "attemptBlocking"
            case _                                 => "succeedNow"
          }

        val cleanReturnType = cleanType(symbol.returnType)
        val trueReturnType =
          methodType match {
            case MethodType.DriverAction           => s"Task[$cleanReturnType]"
            case MethodType.DistributedComputation => s"Task[$cleanReturnType]"
            case _                                 => cleanReturnType
          }

        s"def $name$parameters: $trueReturnType = $transformation(_.$name$arguments)"
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
