package zio.spark.internal.codegen.generation.plan

import zio.{Console, IO, UIO, URIO, ZIO}
import zio.spark.internal.codegen.generation.{Environment, Output}
import zio.spark.internal.codegen.generation.Error.CodegenError
import zio.spark.internal.codegen.generation.Module.{coreModule, sqlModule}
import zio.spark.internal.codegen.generation.template.*

import java.nio.file.Path

object Plan {
  val rddPlan: SparkPlan                    = SparkPlan(coreModule, RDDTemplate)
  val datasetPlan: SparkPlan                = SparkPlan(sqlModule, DatasetTemplate)
  val dataFrameNaFunctionsPlan: SparkPlan   = SparkPlan(sqlModule, DataFrameNaFunctionsTemplate)
  val dataFrameStatFunctionsPlan: SparkPlan = SparkPlan(sqlModule, DataFrameStatFunctionsTemplate)

}

/**
 * Abstract the process of generation that all plan should respect to
 * work.
 */
trait Plan {
  def generatePath: URIO[Environment, Path]

  def generateCode: ZIO[Console & Environment, CodegenError, String]

  def formatCode(code: String): URIO[Environment, String] =
    for {
      environment <- ZIO.service[Environment]
      path        <- generatePath
    } yield environment.scalafmt.format(environment.scalafmtConfiguration, path, code)

  def preValidation: ZIO[Console & Environment, CodegenError, Unit]                = UIO.unit
  def postValidation(code: String): ZIO[Console & Environment, CodegenError, Unit] = UIO.unit

  def generate: ZIO[Console & Environment, CodegenError, Output] =
    for {
      _    <- preValidation
      path <- generatePath
      raw  <- generateCode
      base =
        s"""/**
           | * /!\\ Warning /!\\
           | *
           | * This file is generated using zio-spark-codegen, you should not edit
           | * this file directly.
           | */
           | 
           |$raw""".stripMargin
      code <- formatCode(base)
      _    <- postValidation(code)
    } yield Output(path.toFile, code)
}
