package zio.spark.internal.codegen.structure

import zio.spark.internal.codegen.RDDAnalysis
import zio.spark.internal.codegen.RDDAnalysis.MethodType
import zio.spark.internal.codegen.structure.TypeUtils.*

import scala.meta.*
import scala.meta.contrib.AssociatedComments
import scala.meta.tokens.Token

case class Method(df: Defn.Def, comments: AssociatedComments, path: String) {

  val calls: List[ParameterGroup] = df.paramss.map(ParameterGroup.fromScalaMeta)

  val name: String                   = df.name.value
  val returnType: String             = df.decltpe.get.toString()
  val fullName: String               = s"$path.$name"
  val typeParams: Seq[TypeParameter] = df.tparams.map(TypeParameter.fromScalaMeta)

  def toCode(methodType: MethodType): String =
    methodType match {
      case MethodType.Ignored     => s"[[$fullName]]"
      case MethodType.ToImplement => s"[[$fullName]]"
      case _ =>
        val parameters = calls.map(_.toCode(isArgs = false)).mkString("")
        val arguments  = calls.map(_.toCode(isArgs = true)).mkString("")

        val transformation =
          methodType match {
            case MethodType.DriverAction           => "action"
            case MethodType.DistributedComputation => "action"
            case MethodType.Transformation         => "transformation"
            case _                                 => "succeedNow"
          }

        val cleanReturnType = cleanType(returnType, path)

        val trueReturnType =
          methodType match {
            case MethodType.DriverAction           => s"Task[$cleanReturnType]"
            case MethodType.DistributedComputation => s"Task[$cleanReturnType]"
            case _                                 => cleanReturnType
          }

        val strTypeParams = if (typeParams.nonEmpty) s"[${typeParams.map(_.toCode).mkString(", ")}]" else ""

        val comment = if (comments.leading(df).isEmpty) "" else comments.leading(df).mkString("\n") + "\n"

        s"${comment}def $name$strTypeParams$parameters: $trueReturnType = $transformation(_.$name$arguments)"
    }

  def isSetter: Boolean = name.startsWith("set")
}

object Method {
  sealed trait Kind
  object Kind {
    final case object Function extends Kind
    final case object Setter   extends Kind
  }

  def fromScalaMeta(df: Defn.Def, comments: AssociatedComments, path: String): Method = Method(df, comments, path)
}
