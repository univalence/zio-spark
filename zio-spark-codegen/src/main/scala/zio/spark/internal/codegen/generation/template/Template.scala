package zio.spark.internal.codegen.generation.template

import zio.spark.internal.codegen.generation.Environment
import zio.spark.internal.codegen.generation.MethodType
import zio.spark.internal.codegen.structure.Method

trait Template {
  def name: String

  def typeParameters: List[String]

  def helpers: Helper

  def imports(environment: Environment): Option[String]

  def implicits(environment: Environment): Option[String]

  def annotations(environment: Environment): Option[String]

  final def typeParameter: String = if (typeParameters.nonEmpty) s"[${typeParameters.mkString(", ")}]" else ""

  final def definition: String = {
    val className      = s"$name$typeParameter"
    val underlyingName = s"Underlying$name$typeParameter"

    s"final case class $className(underlying: $underlyingName)"
  }

  def getMethodType(method: Method): MethodType
}

object Template {
  trait Default extends Template { self =>
    override def helpers: Helper = Helper.nothing

    override def typeParameters: List[String] = Nil

    override def imports(environment: Environment): Option[String] = None

    override def implicits(environment: Environment): Option[String] = None

    override def annotations(environment: Environment): Option[String] = None

    override def getMethodType(method: Method): MethodType = MethodType.defaultMethodType(method, self)
  }
}
