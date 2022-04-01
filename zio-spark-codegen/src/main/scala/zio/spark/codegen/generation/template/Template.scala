package zio.spark.codegen.generation.template

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.MethodType
import zio.spark.codegen.structure.Method

trait Template {
  def name: String

  def typeParameters: List[String]

  def helpers: Helper

  def imports(scalaVersion: ScalaBinaryVersion): Option[String]

  def implicits(scalaVersion: ScalaBinaryVersion): Option[String]

  def annotations(scalaVersion: ScalaBinaryVersion): Option[String]

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

    override def imports(scalaVersion: ScalaBinaryVersion): Option[String] = None

    override def implicits(scalaVersion: ScalaBinaryVersion): Option[String] = None

    override def annotations(scalaVersion: ScalaBinaryVersion): Option[String] = None

    override def getMethodType(method: Method): MethodType = MethodType.defaultMethodType(method, self)
  }
}
