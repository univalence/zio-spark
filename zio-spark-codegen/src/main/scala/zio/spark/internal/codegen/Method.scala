package zio.spark.internal.codegen

import zio.spark.internal.codegen.RDDAnalysis.MethodType

import scala.reflect.runtime.universe

case class Method(
    name:        String,
    path:        String,
    calls:       List[Call],
    returnType:  String,
    annotations: List[String],
    kind:        Method.Kind
) {
  val fullName = s"$path.$name"

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

        val trueReturnType =
          methodType match {
            case MethodType.DriverAction           => s"Task[$returnType]"
            case MethodType.DistributedComputation => s"Task[$returnType]"
            case _                                 => returnType
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

  def fromSymbol(symbol: universe.MethodSymbol): Method =
    Method(
      name        = symbol.name.toString,
      path        = symbol.fullName.split("\\.").dropRight(1).mkString("."),
      calls       = symbol.paramLists.map(Call.fromSymbol),
      returnType  = symbol.returnType.typeSymbol.fullName,
      annotations = symbol.annotations.map(_.toString),
      kind        = if (symbol.isSetter) Kind.Setter else Kind.Function
    )
}
