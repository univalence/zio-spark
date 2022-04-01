package zio.spark.codegen.generation

import zio.{IO, ZIO}
import zio.spark.codegen.generation.Environment.Environment
import zio.spark.codegen.generation.Error.{CodegenError, WriteError}
import zio.spark.codegen.generation.plan.Plan

/**
 * generate a list of plan describing how and where SBT should generate
 * a particular file.
 */
case object Generator {
  def generateAll(plans: Seq[Plan]): ZIO[Environment, Nothing, Seq[Output]] =
    for {
      outputs <- ZIO.foreachPar(plans)(generate)
      _ <-
        if (outputs.exists(_.isEmpty)) {
          ZIO.die(new Throwable("The ZIO Spark generation is canceled because we found errors."))
        } else {
          ZIO.unit
        }
    } yield outputs.collect { case Some(v) => v }

  def write(output: Output): IO[CodegenError, Unit] =
    ZIO.attempt(sbt.IO.write(output.file, output.code)).mapError(e => WriteError(output.file.toString, e))

  def generate(plan: Plan): ZIO[Environment, Nothing, Option[Output]] = {
    val effect =
      for {
        output <- plan.generate
        _      <- write(output)
      } yield output

    effect
      .tapBoth(
        error => Logger.error(s"""The plan ${plan.name} can't be generated, it is canceled because:
                                 |${error.fullMsg.split("\n").map("    " + _).mkString("\n")}""".stripMargin),
        output => Logger.success(s"Successfully generated the plan for '${plan.name}' at '${output.file.toString}'.")
      )
      .fold(
        _ => None,
        output => Some(output)
      )
  }

}
