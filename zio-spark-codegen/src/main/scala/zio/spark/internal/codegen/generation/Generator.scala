package zio.spark.internal.codegen.generation

import zio.{Console, ZIO}
import zio.spark.internal.codegen.generation.Error.CodegenError
import zio.spark.internal.codegen.generation.plan.Plan

/**
 * generate a list of plan describing how and where SBT should generate
 * a particular file.
 */
case class Generator(plans: Seq[Plan]) {
  def generate: ZIO[Console & Environment, CodegenError, Seq[Output]] = ZIO.foreachPar(plans)(_.generate)
}
